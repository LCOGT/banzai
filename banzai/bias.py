import os.path
import logging

import numpy as np

from banzai.stages import Stage
from banzai.calibrations import CalibrationStacker, ApplyCalibration, CalibrationComparer
from banzai.utils import stats, fits_utils

logger = logging.getLogger(__name__)


class BiasMaker(CalibrationStacker):

    def __init__(self, pipeline_context):
        super(BiasMaker, self).__init__(pipeline_context)

    @property
    def group_by_attributes(self):
        return ['ccdsum']

    @property
    def calibration_type(self):
        return 'BIAS'

    @property
    def min_images(self):
        return 5

    def make_master_calibration_frame(self, images):
        master_image = super(BiasMaker, self).make_master_calibration_frame(images)
        master_image.header['BIASLVL'] = (np.mean([image.header['BIASLVL'] for image in images]),
                                          'Mean bias level of master bias')
        return master_image


class BiasSubtractor(ApplyCalibration):
    def __init__(self, pipeline_context):
        super(BiasSubtractor, self).__init__(pipeline_context)

    @property
    def master_selection_criteria(self):
        return ['ccdsum']

    @property
    def calibration_type(self):
        return 'bias'

    def apply_master_calibration(self, images, master_calibration_image):

        master_bias_data = master_calibration_image.data
        master_bias_level = float(master_calibration_image.header['BIASLVL'])

        master_logging_tags = {'bias': master_bias_level,
                               'master_bias': os.path.basename(master_calibration_image.filename)}

        for image in images:
            image.subtract(master_bias_level)
            image.subtract(master_bias_data)

            image.bpm |= master_calibration_image.bpm
            image.header['BIASLVL'] = (master_bias_level, 'Mean bias level of master bias')

            master_bias_filename = os.path.basename(master_calibration_image.filename)
            image.header['L1IDBIAS'] = (master_bias_filename, 'ID of bias frame used')
            image.header['L1STATBI'] = (1, "Status flag for bias frame correction")
            logging_tags = {'BIASLVL': image.header['BIASLVL'],
                            'L1IDBIAS': image.header['L1IDBIAS']}
            logging_tags.update(master_logging_tags)

            logger.info('Subtracting bias', image=image,  extra_tags=logging_tags)
        return images


class OverscanSubtractor(Stage):
    def __init__(self, pipeline_context):
        super(OverscanSubtractor, self).__init__(pipeline_context)

    def do_stage(self, images):

        for image in images:
            # Subtract the overscan if it exists
            if image.data_is_3d():
                logging_tags = {}
                for i in range(image.get_n_amps()):
                    overscan_level = _subtract_overscan_3d(image, i)
                    logging_tags['OVERSCN{0}'.format(i + 1)] = float(overscan_level)
            else:
                overscan_level = _subtract_overscan_2d(image)
                logging_tags = {'OVERSCAN': float(overscan_level)}
            logger.info('Subtracting overscan', image=image, extra_tags=logging_tags)

        return images


class BiasMasterLevelSubtractor(Stage):
    def __init__(self, pipeline_context):
        super(BiasMasterLevelSubtractor, self).__init__(pipeline_context)

    def do_stage(self, images):

        for image in images:
            bias_level = stats.sigma_clipped_mean(image.data, 3.5, mask=image.bpm)
            logger.debug('Subtracting bias level', image=image, extra_tags={'BIASLVL': float(bias_level)})
            image.data -= bias_level
            image.header['BIASLVL'] = bias_level, 'Bias Level that was removed'

        return images


class BiasComparer(CalibrationComparer):
    def __init__(self, pipeline_context):
        super(BiasComparer, self).__init__(pipeline_context)

    @property
    def master_selection_criteria(self):
        return ['ccdsum']

    @property
    def calibration_type(self):
        return 'bias'

    @property
    def reject_images(self):
        return True

    def noise_model(self, image):
        return image.readnoise


def _subtract_overscan_3d(image, i):
    overscan_region = fits_utils.parse_region_keyword(image.extension_headers[i].get('BIASSEC'))
    if overscan_region is not None:
        overscan_level = stats.sigma_clipped_mean(image.data[i][overscan_region], 3)
        image.header['L1STATOV'] = (1, 'Status flag for overscan correction')
    else:
        overscan_level = 0.0
        image.header['L1STATOV'] = (0, 'Status flag for overscan correction')

    overscan_comment = 'Overscan value that was subtracted from Q{0}'.format(i + 1)
    image.header['OVERSCN{0}'.format(i + 1)] = (overscan_level, overscan_comment)

    image.data[i] -= overscan_level
    return overscan_level


def _subtract_overscan_2d(image):
    overscan_region = fits_utils.parse_region_keyword(image.header.get('BIASSEC'))
    if overscan_region is not None:
        overscan_level = stats.sigma_clipped_mean(image.data[overscan_region], 3)
        image.header['L1STATOV'] = (1, 'Status flag for overscan correction')
    else:
        overscan_level = 0.0
        image.header['L1STATOV'] = (0, 'Status flag for overscan correction')

    image.header['OVERSCAN'] = (overscan_level, 'Overscan value that was subtracted')
    image.data -= overscan_level
    return overscan_level
