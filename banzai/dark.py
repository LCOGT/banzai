import os.path
import logging

import numpy as np

from banzai.stages import Stage
from banzai.calibrations import CalibrationStacker, ApplyCalibration, CalibrationComparer

logger = logging.getLogger(__name__)


class DarkNormalizer(Stage):
    def __init__(self, pipeline_context):
        super(DarkNormalizer, self).__init__(pipeline_context)

    def do_stage(self, images):
        for image in images:
            image.data /= image.exptime
            logger.info('Normalizing dark by exposure time', image=image)
        return images


class DarkMaker(CalibrationStacker):
    def __init__(self, pipeline_context):
        super(DarkMaker, self).__init__(pipeline_context)

    @property
    def group_by_attributes(self):
        return ['ccdsum']

    @property
    def calibration_type(self):
        return 'DARK'

    @property
    def min_images(self):
        return 5


class DarkSubtractor(ApplyCalibration):
    def __init__(self, pipeline_context):
        super(DarkSubtractor, self).__init__(pipeline_context)

    @property
    def calibration_type(self):
        return 'dark'

    @property
    def master_selection_criteria(self):
        return ['ccdsum']

    def apply_master_calibration(self, images, master_calibration_image):
        master_dark_data = master_calibration_image.data
        master_dark_filename = os.path.basename(master_calibration_image.filename)
        logging_tags = {'master_dark': os.path.basename(master_calibration_image.filename)}

        for image in images:
            logger.info('Subtracting dark', image=image, extra_tags=logging_tags)
            image.data -= master_dark_data * image.exptime
            image.bpm |= master_calibration_image.bpm
            image.header['L1IDDARK'] = (master_dark_filename, 'ID of dark frame used')
            image.header['L1STATDA'] = (1, 'Status flag for dark frame correction')
        return images


class DarkComparer(CalibrationComparer):
    def __init__(self, pipeline_context):
        super(DarkComparer, self).__init__(pipeline_context)

    @property
    def master_selection_criteria(self):
        return ['ccdsum']

    @property
    def calibration_type(self):
        return 'dark'

    @property
    def reject_images(self):
        return True

    def noise_model(self, image):
        poisson_noise = np.where(image.data > 0, image.data * image.exptime, 0.0)
        noise = (image.readnoise ** 2.0 + poisson_noise) ** 0.5
        noise /= image.exptime
        return noise
