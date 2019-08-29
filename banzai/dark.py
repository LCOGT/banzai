import os.path
import logging

import numpy as np

from banzai.stages import Stage
from banzai.calibrations import CalibrationStacker, CalibrationUser, CalibrationComparer
from banzai.utils import qc

logger = logging.getLogger('banzai')


class DarkNormalizer(Stage):
    def __init__(self, runtime_context):
        super(DarkNormalizer, self).__init__(runtime_context)

    def do_stage(self, image):
        if image.exptime <= 0.0:
            logger.error('EXPTIME is <= 0.0. Rejecting frame', image=image)
            qc_results = {'exptime': image.exptime, 'rejected': True}
            qc.save_qc_results(self.runtime_context, qc_results, image)
            return None
        image.data /= image.exptime
        logger.info('Normalizing dark by exposure time', image=image)
        return image


class DarkMaker(CalibrationStacker):
    def __init__(self, runtime_context):
        super(DarkMaker, self).__init__(runtime_context)

    @property
    def calibration_type(self):
        return 'DARK'


class DarkSubtractor(CalibrationUser):
    def __init__(self, runtime_context):
        super(DarkSubtractor, self).__init__(runtime_context)

    @property
    def calibration_type(self):
        return 'dark'

    def apply_master_calibration(self, image, master_calibration_image):
        master_dark_data = master_calibration_image.data
        master_dark_filename = os.path.basename(master_calibration_image.filename)
        logging_tags = {'master_dark': os.path.basename(master_calibration_image.filename)}

        logger.info('Subtracting dark', image=image, extra_tags=logging_tags)
        image.data -= master_dark_data * image.exptime
        image.bpm |= master_calibration_image.bpm
        image.header['L1IDDARK'] = (master_dark_filename, 'ID of dark frame')
        image.header['L1STATDA'] = (1, 'Status flag for dark frame correction')
        return image


class DarkComparer(CalibrationComparer):
    def __init__(self, runtime_context):
        super(DarkComparer, self).__init__(runtime_context)

    @property
    def calibration_type(self):
        return 'dark'

    @property
    def reject_image(self):
        return True

    def noise_model(self, image):
        poisson_noise = np.where(image.data > 0, image.data * image.exptime, 0.0)
        noise = (image.readnoise ** 2.0 + poisson_noise) ** 0.5
        noise /= image.exptime
        return noise
