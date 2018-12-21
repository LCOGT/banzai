import logging
import abc
import itertools
import os

import numpy as np

from banzai.stages import Stage
from banzai import dbs, logs
from banzai.utils import image_utils, stats, fits_utils

logger = logging.getLogger(__name__)


class CalibrationMaker(Stage):
    def __init__(self, pipeline_context):
        super(CalibrationMaker, self).__init__(pipeline_context)
        self.group_by_attributes = self.pipeline_context.CALIBRATION_SET_CRITERIA.get(self.calibration_type.upper(), [])

    @property
    @abc.abstractmethod
    def calibration_type(self):
        pass

    @abc.abstractmethod
    def make_master_calibration_frame(self, images, image_config):
        pass

    def get_grouping(self, image):
        grouping_criteria = [image.site, image.camera, image.epoch]
        if self.group_by_attributes:
            grouping_criteria += [getattr(image, keyword) for keyword in self.group_by_attributes]
        return grouping_criteria

    def run(self, images):
        images.sort(key=self.get_grouping)
        processed_images = []
        for _, image_set in itertools.groupby(images, self.get_grouping):
            try:
                image_set = list(image_set)
                logger.info('Running {0}'.format(self.stage_name), image=image_set[0])
                processed_images += self.do_stage(image_set)
            except Exception:
                logger.error(logs.format_exception())
        return processed_images

    def do_stage(self, images):
        min_images = self.pipeline_context.CALIBRATION_MIN_IMAGES.get(self.calibration_type.upper(), -1)
        if len(images) < min_images:
            # Do nothing
            logger.warning('Number of images less than minimum requirement of {min_images}, not combining'.format(
                min_images=min_images))
            return []
        else:
            image_utils.check_image_homogeneity(images, self.group_by_attributes)
            return [self.make_master_calibration_frame(images)]

    def get_calibration_filename(self, image):
        cal_file = '{site}{telescop}-{camera}-{epoch}-{cal_type}'.format(
            site=image.site,
            telescop=image.header.get('TELESCOP').replace('-', ''),
            camera=image.camera,
            epoch=image.epoch,
            cal_type=self.calibration_type.lower(),
        )
        for attribute in sorted(self.group_by_attributes):
            attribute_value = getattr(image, attribute)
            if attribute == 'ccdsum':
                attribute_value = 'bin{ccdsum}'.format(ccdsum=attribute_value.replace(' ', 'x'))
            cal_file += '-{attribute_value}'.format(attribute_value=attribute_value)
        cal_file += '.fits'
        return cal_file


class CalibrationStacker(CalibrationMaker):
    def __init__(self, pipeline_context):
        super(CalibrationStacker, self).__init__(pipeline_context)

    def make_master_calibration_frame(self, images):
        data_stack = np.zeros((images[0].ny, images[0].nx, len(images)), dtype=np.float32)
        stack_mask = np.zeros((images[0].ny, images[0].nx, len(images)), dtype=np.uint8)

        master_calibration_filename = self.get_calibration_filename(images[0])

        for i, image in enumerate(images):
            logger.debug('Stacking Frames', image=image,
                         extra_tags={'master_calibration': os.path.basename(master_calibration_filename)})
            data_stack[:, :, i] = image.data[:, :]
            stack_mask[:, :, i] = image.bpm[:, :]

        stacked_data = stats.sigma_clipped_mean(data_stack, 3.0, axis=2, mask=stack_mask, inplace=True)

        # Memory cleanup
        del data_stack
        del stack_mask

        master_bpm = np.array(stacked_data == 0.0, dtype=np.uint8)

        # Save the master dark image with all of the combined images in the header
        master_header = fits_utils.create_master_calibration_header(images)
        master_image = self.pipeline_context.FRAME_CLASS(self.pipeline_context, data=stacked_data, header=master_header)
        master_image.filename = master_calibration_filename
        master_image.bpm = master_bpm

        logger.info('Created master calibration stack', image=master_image,
                    extra_tags={'calibration_type': self.calibration_type})
        return master_image


class MasterCalibrationDoesNotExist(Exception):
    pass


class ApplyCalibration(Stage):
    def __init__(self, pipeline_context):
        super(ApplyCalibration, self).__init__(pipeline_context)
        self.master_selection_criteria = self.pipeline_context.CALIBRATION_SET_CRITERIA.get(
            self.calibration_type.upper(), [])

    @property
    @abc.abstractmethod
    def calibration_type(self):
        pass

    def on_missing_master_calibration(self, image):
        logger.error('Master Calibration file does not exist for {stage}'.format(stage=self.stage_name), image=image)
        raise MasterCalibrationDoesNotExist

    def get_grouping(self, image):
        grouping_criteria = [image.site, image.camera, image.epoch]
        if self.master_selection_criteria:
            grouping_criteria += [getattr(image, keyword) for keyword in self.master_selection_criteria]
        return grouping_criteria

    def run(self, images):
        images.sort(key=self.get_grouping)
        processed_images = []
        for _, image_set in itertools.groupby(images, self.get_grouping):
            try:
                image_set = list(image_set)
                logger.info('Running {0}'.format(self.stage_name), image=image_set[0])
                processed_images += self.do_stage(image_set)
            except Exception:
                logger.error(logs.format_exception())
        return processed_images

    def do_stage(self, images):
        if len(images) == 0:
            # Abort!
            return []
        else:
            image_utils.check_image_homogeneity(images)
            master_calibration_filename = self.get_calibration_filename(images[0])

            if master_calibration_filename is None:
                self.on_missing_master_calibration(images[0])

            master_calibration_image = self.pipeline_context.FRAME_CLASS(self.pipeline_context,
                                                                         filename=master_calibration_filename)
            return self.apply_master_calibration(images, master_calibration_image)

    @abc.abstractmethod
    def apply_master_calibration(self, images, master_calibration_image):
        pass

    def get_calibration_filename(self, image):
        return dbs.get_master_calibration_image(image, self.calibration_type, self.master_selection_criteria,
                                                db_address=self.pipeline_context.db_address)


class CalibrationComparer(ApplyCalibration):
    # In a 16 megapixel image, this should flag 0 or 1 pixels statistically, much much less than 5% of the image
    SIGNAL_TO_NOISE_THRESHOLD = 6.0
    ACCEPTABLE_PIXEL_FRACTION = 0.05

    @property
    @abc.abstractmethod
    def reject_images(self):
        pass

    def on_missing_master_calibration(self, image):
        msg = 'No master {caltype} frame exists. Assuming these images are ok.'
        logger.warning(msg.format(caltype=self.calibration_type), image=image)

    def apply_master_calibration(self, images, master_calibration_image):
        # Short circuit
        if master_calibration_image.data is None:
            return images

        images_to_reject = []

        for image in images:
            # We assume the images have already been normalized before this stage is run.
            bad_pixel_fraction = np.abs(image.data - master_calibration_image.data)
            # Estimate the noise of the image
            noise = self.noise_model(image)
            bad_pixel_fraction /= noise
            bad_pixel_fraction = bad_pixel_fraction >= self.SIGNAL_TO_NOISE_THRESHOLD
            bad_pixel_fraction = bad_pixel_fraction.sum() / float(bad_pixel_fraction.size)
            frame_is_bad = bad_pixel_fraction > self.ACCEPTABLE_PIXEL_FRACTION

            qc_results = {"master_comparison.fraction": bad_pixel_fraction,
                          "master_comparison.snr_threshold": self.SIGNAL_TO_NOISE_THRESHOLD,
                          "master_comparison.pixel_threshold": self.ACCEPTABLE_PIXEL_FRACTION,
                          "master_comparison.comparison_master_filename": master_calibration_image.filename}

            logging_tags = {}
            for qc_check, qc_result in qc_results.items():
                logging_tags[qc_check] = qc_result
            logging_tags['master_comparison_filename'] = master_calibration_image.filename
            msg = "Performing comparison to last good master {caltype} frame"
            logger.info(msg.format(caltype=self.calibration_type), image=image, extra_tags=logging_tags)

            # This needs to be added after the qc_results dictionary is used for the logging tags because
            # they can't handle booleans
            qc_results["master_comparison.failed"] = frame_is_bad
            if frame_is_bad:
                # Reject the image and log an error
                images_to_reject.append(image)
                qc_results['rejected'] = True
                msg = 'Rejecting {caltype} image because it deviates too much from the previous master'
                logger.error(msg.format(caltype=self.calibration_type), image=image, extra_tags=logging_tags)

            self.save_qc_results(qc_results, image)

        if self.reject_images and not self.pipeline_context.preview_mode:
            for image_to_reject in images_to_reject:
                images.remove(image_to_reject)
        return images

    @abc.abstractmethod
    def noise_model(self, image):
        return np.ones(image.data.size)
