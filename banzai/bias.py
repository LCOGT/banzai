import numpy as np

from banzai.calibrations import CalibrationStacker, CalibrationUser, CalibrationComparer
from banzai.stages import Stage
from banzai.utils import stats
from banzai.logs import get_logger

logger = get_logger()


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
        image -= master_calibration_image.bias_level * image.n_sub_exposures
        image.meta['BIASLVL'] = master_calibration_image.bias_level, 'Bias level that was removed after overscan'
        image -= master_calibration_image * image.n_sub_exposures
        image.meta['L1IDBIAS'] = master_calibration_image.filename, 'ID of bias frame'
        image.meta['L1STATBI'] = 1, "Status flag for bias frame correction"
        return image


class OverscanSubtractor(Stage):
    def __init__(self, runtime_context):
        super(OverscanSubtractor, self).__init__(runtime_context)

    def do_stage(self, image):
        for data in image.ccd_hdus:
            overscan_section = data.get_overscan_region()
            if overscan_section is not None:
                overscan_level = stats.sigma_clipped_mean(data.data[overscan_section.to_slice()], 3)
                data -= overscan_level
                data.meta['L1STATOV'] = '1', 'Status flag for overscan correction'
                data.meta['OVERSCAN'] = overscan_level, 'Overscan value that was subtracted'
            else:
                data.meta['L1STATOV'] = 0, 'Status flag for overscan correction'
                data.meta['OVERSCAN'] = 0.0

        return image


class BiasMasterLevelSubtractor(Stage):
    def __init__(self, runtime_context):
        super(BiasMasterLevelSubtractor, self).__init__(runtime_context)

    def do_stage(self, image):
        bias_level = stats.sigma_clipped_mean(image.data, 3.5, mask=image.mask)
        image -= bias_level
        image.meta['BIASLVL'] = bias_level / image.n_sub_exposures, 'Bias level that was removed after overscan'
        return image


class BiasComparer(CalibrationComparer):
    def __init__(self, runtime_context):
        super(BiasComparer, self).__init__(runtime_context)

    @property
    def calibration_type(self):
        return 'bias'
