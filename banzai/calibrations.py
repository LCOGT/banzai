import logging
import abc
import itertools

import numpy as np

from banzai.stages import Stage
from banzai import dbs
from banzai.images import Image
from banzai.utils import image_utils

logger = logging.getLogger(__name__)


class CalibrationMaker(Stage):
    def __init__(self, pipeline_context):
        super(CalibrationMaker, self).__init__(pipeline_context)

    @property
    @abc.abstractmethod
    def calibration_type(self):
        pass

    @abc.abstractmethod
    def make_master_calibration_frame(self, images, image_config):
        pass

    @property
    @abc.abstractmethod
    def min_images(self):
        return 5

    @property
    @abc.abstractmethod
    def group_by_keywords(self):
        return []

    def get_grouping(self, image):
        grouping_criteria = [image.site, image.instrument, image.epoch]
        if self.group_by_keywords:
            grouping_criteria += [image.header[keyword] for keyword in self.group_by_keywords]
        return grouping_criteria

    def run_stage(self, image_set):
        image_set = list(image_set)
        logger.info('Running {0}'.format(self.stage_name), image=image_set[0])
        return self.do_stage(image_set)

    def run(self, images):
        # Take out any images that are None
        images = [image for image in images if image is not None]
        images.sort(key=self.get_grouping)
        processed_images = []
        for _, image_set in itertools.groupby(images, self.get_grouping):
            try:
                processed_images += [self.run_stage(image_set)]
            except Exception as e:
                logger.error(e)
        return processed_images

    def do_stage(self, images):
        if len(images) < self.min_images:
            # Do nothing
            logger.warning('Not enough images to combine.')
            return []
        else:
            image_utils.check_image_homogeneity(images, self.group_by_keywords)

            return self.make_master_calibration_frame(images, images[0])

    def get_calibration_filename(self, image):
        cal_file = '{cal_type}_{instrument}_{epoch}_bin{bin}{filter}.fits'
        if 'filter' in self.group_by_attributes:
            filter_str = '_{filter}'.format(filter=image.filter)
        else:
            filter_str = ''

        cal_file = cal_file.format(instrument=image.instrument,
                                   epoch=image.epoch, bin=image.ccdsum.replace(' ', 'x'),
                                   cal_type=self.calibration_type.lower(), filter=filter_str)
        return cal_file


class MasterCalibrationDoesNotExist(Exception):
    pass


class ApplyCalibration(Stage):
    def __init__(self, pipeline_context):
        super(ApplyCalibration, self).__init__(pipeline_context)

    @property
    @abc.abstractmethod
    def calibration_type(self):
        pass

    @property
    @abc.abstractmethod
    def image_attribute_keywords(self):
        return []

    def on_missing_master_calibration(self, image):
        logger.error('Master Calibration file does not exist for {stage}'.format(stage=self.stage_name), image=image)
        raise MasterCalibrationDoesNotExist

    def do_stage(self, image):
        master_calibration_filename = self.get_calibration_filename(image)

        if master_calibration_filename is None:
            self.on_missing_master_calibration(image)

        master_calibration_image = Image(self.pipeline_context,
                                         filename=master_calibration_filename)
        image_utils.check_image_homogeneity([image, master_calibration_image],
                                            self.image_attribute_keywords)
        return self.apply_master_calibration(image, master_calibration_image)

    @abc.abstractmethod
    def apply_master_calibration(self, images, master_calibration_image):
        pass

    def get_calibration_filename(self, image):
        return dbs.get_master_calibration_image(image, self.calibration_type, self.image_attribute_keywords,
                                                db_address=self.pipeline_context.db_address)


class CalibrationComparer(ApplyCalibration):
    # In a 16 megapixel image, this should flag 0 or 1 pixels statistically, much much less than 5% of the image
    SIGNAL_TO_NOISE_THRESHOLD = 6.0
    ACCEPTABLE_PIXEL_FRACTION = 0.05

    @property
    @abc.abstractmethod
    def reject_images(self):
        pass

    def __init__(self, pipeline_context):
        super(ApplyCalibration, self).__init__(pipeline_context)

    def on_missing_master_calibration(self, image):
        msg = 'No master {caltype} frame exists. Assuming these images are ok.'
        logger.warning(msg.format(caltype=self.calibration_type), image=image)

    def apply_master_calibration(self, image, master_calibration_image):
        # Short circuit
        if master_calibration_image.data is None:
            return image

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
        qc_results['rejected'] = frame_is_bad
        self.save_qc_results(qc_results, image)

        if frame_is_bad:
            # Reject the image and log an error
            msg = 'Rejecting {caltype} image because it deviates too much from the previous master'
            logger.error(msg.format(caltype=self.calibration_type), image=image, extra_tags=logging_tags)
            return None

        return image

    @abc.abstractmethod
    def noise_model(self, image):
        return np.ones(image.data.size)
