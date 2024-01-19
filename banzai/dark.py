from banzai.stages import Stage
from banzai.calibrations import CalibrationStacker, CalibrationUser, CalibrationComparer
from banzai.utils import qc
from banzai.logs import get_logger
import numpy as np

logger = get_logger()


class DarkNormalizer(Stage):
    def __init__(self, runtime_context):
        super(DarkNormalizer, self).__init__(runtime_context)

    def do_stage(self, image):
        if image.exptime <= 0.0:
            logger.error('EXPTIME is <= 0.0. Rejecting frame', image=image)
            qc_results = {'exptime': image.exptime, 'rejected': True}
            qc.save_qc_results(self.runtime_context, qc_results, image)
            return None
        image /= image.exptime
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
        master_calibration_image *= image.exptime
        temperature_scaling_factor = np.exp(master_calibration_image.dark_temperature_coefficient * \
                                            (image.measured_ccd_temperature - master_calibration_image.measured_ccd_temperature))
        master_calibration_image *= temperature_scaling_factor
        image -= master_calibration_image
        image.meta['L1IDDARK'] = master_calibration_image.filename, 'ID of dark frame'
        image.meta['L1STATDA'] = 1, 'Status flag for dark frame correction'
        image.meta['DRKTSCAL'] = temperature_scaling_factor, 'Temperature scaling factor applied to dark image'
        return image


class DarkComparer(CalibrationComparer):
    def __init__(self, runtime_context):
        super(DarkComparer, self).__init__(runtime_context)

    @property
    def calibration_type(self):
        return 'dark'


class DarkTemperatureChecker(Stage):
    def __init__(self, runtime_context):
        super(DarkTemperatureChecker, self).__init__(runtime_context)

    def do_stage(self, image):
        temperature_matches = abs(image.requested_ccd_temperature - image.measured_ccd_temperature) < 0.5
        if not temperature_matches:
            image.is_bad = True
            logger.error('Marking frame as bad because its set temperature is more than 0.5 degrees off the actual',
                         image=image)
        return image
