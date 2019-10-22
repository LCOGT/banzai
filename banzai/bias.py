import logging
import numpy as np

from banzai.calibrations import CalibrationStacker, CalibrationUser, CalibrationComparer
from banzai.stages import Stage
from banzai.utils import stats

logger = logging.getLogger('banzai')


class BiasMaker(CalibrationStacker):

    def __init__(self, runtime_context):
        super(BiasMaker, self).__init__(runtime_context)

    @property
    def calibration_type(self):
        return 'BIAS'

    def make_master_calibration_frame(self, images):
        master_image = super(BiasMaker, self).make_master_calibration_frame(images)
        master_image.bias_level = np.mean([image.bias_level for image in images if image.bias_level is not None])

        return master_image


class BiasSubtractor(CalibrationUser):
    def __init__(self, runtime_context):
        super(BiasSubtractor, self).__init__(runtime_context)

    @property
    def calibration_type(self):
        return 'bias'

    def apply_master_calibration(self, image, master_calibration_image):
        logger.info('Subtracting master bias', image=image)
        image.subtract(master_calibration_image.bias_level, kind='bias_level')
        return image


class OverscanSubtractor(Stage):
    def __init__(self, runtime_context):
        super(OverscanSubtractor, self).__init__(runtime_context)

    def do_stage(self, image):
        for data in image.ccd_hdus:
            overscan_section = data.get_overscan_region()
            if overscan_section is not None:
                data.subtract(stats.sigma_clipped_mean(data.data[overscan_section.to_slice()], 3),
                              kind='overscan')
        return image


class BiasMasterLevelSubtractor(Stage):
    def __init__(self, runtime_context):
        super(BiasMasterLevelSubtractor, self).__init__(runtime_context)

    def do_stage(self, image):
        image.subtract(stats.sigma_clipped_mean(image.data, 3.5, mask=image.mask), kind='bias_level')
        return image


class BiasComparer(CalibrationComparer):
    def __init__(self, runtime_context):
        super(BiasComparer, self).__init__(runtime_context)

    @property
    def calibration_type(self):
        return 'bias'
