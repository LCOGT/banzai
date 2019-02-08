import logging
import abc
import os

import numpy as np

from banzai.stages import Stage, MultiFrameStage
from banzai import dbs, logs
from banzai.utils import image_utils, stats, fits_utils, qc

logger = logging.getLogger(__name__)


class CalibrationMaker(MultiFrameStage):
    def __init__(self, pipeline_context):
        super(CalibrationMaker, self).__init__(pipeline_context)

    def group_by_attributes(self):
        return self.pipeline_context.CALIBRATION_SET_CRITERIA.get(self.calibration_type.upper(), [])

    @property
    @abc.abstractmethod
    def calibration_type(self):
        pass

    @abc.abstractmethod
    def make_master_calibration_frame(self, images):
        pass

    def do_stage(self, images):
        try:
            min_images = self.pipeline_context.CALIBRATION_MIN_FRAMES[self.calibration_type.upper()]
        except KeyError:
            msg = 'The minimum number of frames required to create a master calibration of type ' \
                  '{calibration_type} has not been specified in the settings.'
            logger.error(msg.format(calibration_type=self.calibration_type.upper()))
            return []
        if len(images) < min_images:
            # Do nothing
            msg = 'Number of images less than minimum requirement of {min_images}, not combining'
            logger.warning(msg.format(min_images=min_images))
            return []
        try:
            image_utils.check_image_homogeneity(images, self.group_by_attributes())
        except image_utils.InhomogeneousSetException:
            logger.error(logs.format_exception())
            return []

        return [self.make_master_calibration_frame(images)]


class CalibrationStacker(CalibrationMaker):
    def __init__(self, pipeline_context):
        super(CalibrationStacker, self).__init__(pipeline_context)

    def make_master_calibration_frame(self, images):
        data_stack = np.zeros((images[0].ny, images[0].nx, len(images)), dtype=np.float32)
        stack_mask = np.zeros((images[0].ny, images[0].nx, len(images)), dtype=np.uint8)

        make_calibration_name = self.pipeline_context.CALIBRATION_FILENAME_FUNCTIONS[self.calibration_type]

        master_calibration_filename = make_calibration_name(images[0])

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


class ApplyCalibration(Stage):
    def __init__(self, pipeline_context):
        super(ApplyCalibration, self).__init__(pipeline_context)

    @property
    def master_selection_criteria(self):
        return self.pipeline_context.CALIBRATION_SET_CRITERIA.get(self.calibration_type.upper(), [])

    @property
    @abc.abstractmethod
    def calibration_type(self):
        pass

    def on_missing_master_calibration(self, image):
        msg = 'Master Calibration file does not exist for {stage}, flagging image as bad'
        logger.error(msg.format(stage=self.stage_name), image=image)
        image.is_bad = True

    def do_stage(self, image):
        master_calibration_filename = self.get_calibration_filename(image)

        if master_calibration_filename is None:
            self.on_missing_master_calibration(image)
            return image

        master_calibration_image = self.pipeline_context.FRAME_CLASS(self.pipeline_context,
                                                                     filename=master_calibration_filename)
        try:
            image_utils.check_image_homogeneity([image, master_calibration_image], self.master_selection_criteria)
        except image_utils.InhomogeneousSetException:
            logger.error(logs.format_exception(), image=image)
            return None

        return self.apply_master_calibration(image, master_calibration_image)

    @abc.abstractmethod
    def apply_master_calibration(self, image, master_calibration_image):
        pass

    def get_calibration_filename(self, image):
        return dbs.get_master_calibration_image(image, self.calibration_type, self.master_selection_criteria,
                                                realtime_reduction=self.pipeline_context.preview_mode,
                                                db_address=self.pipeline_context.db_address)


class CalibrationComparer(ApplyCalibration):
    # In a 16 megapixel image, this should flag 0 or 1 pixels statistically, much much less than 5% of the image
    SIGNAL_TO_NOISE_THRESHOLD = 6.0
    ACCEPTABLE_PIXEL_FRACTION = 0.05

    @property
    @abc.abstractmethod
    def reject_image(self):
        pass

    def on_missing_master_calibration(self, image):
        msg = 'No master {caltype} frame exists, flagging image as bad.'
        logger.error(msg.format(caltype=self.calibration_type), image=image)
        image.is_bad = True

    def apply_master_calibration(self, image, master_calibration_image):
        # Short circuit
        if master_calibration_image.data is None:
            return image

        # We assume the image has already been normalized before this stage is run.
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
            # Flag the image as bad and log an error
            image.is_bad = True
            qc_results['rejected'] = True
            msg = 'Flagging {caltype} as bad because it deviates too much from the previous master'
            logger.error(msg.format(caltype=self.calibration_type), image=image, extra_tags=logging_tags)

        qc.save_qc_results(self.pipeline_context, qc_results, image)

        return image

    @abc.abstractmethod
    def noise_model(self, image):
        return np.ones(image.data.size)


def make_calibration_filename_function(calibration_type, attribute_filename_functions, telescope_filename_function):
    def get_calibration_filename(image):
        name_components = {'site': image.site, 'telescop': telescope_filename_function(image),
                           'camera': image.header.get('INSTRUME', ''), 'epoch': image.epoch,
                           'cal_type': calibration_type.lower()}
        cal_file = '{site}{telescop}-{camera}-{epoch}-{cal_type}'.format(**name_components)
        for filename_function in attribute_filename_functions:
            cal_file += '-{}'.format(filename_function(image))
        cal_file += '.fits'
        return cal_file
    return get_calibration_filename
