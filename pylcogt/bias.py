from __future__ import absolute_import, print_function, division

import os.path

import numpy as np

from pylcogt.images import Image
from . import logs
from .stages import CalibrationMaker, ApplyCalibration, Stage
from .utils import stats, fits_utils


__author__ = 'cmccully'


class BiasMaker(CalibrationMaker):

    def __init__(self, pipeline_context):
        super(BiasMaker, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return ['ccdsum']

    @property
    def calibration_type(self):
        return 'BIAS'

    @property
    def min_images(self):
        return 5

    def make_master_calibration_frame(self, images, image_config, logging_tags):

        bias_data = np.zeros((image_config.ny, image_config.nx, len(images)))

        bias_mask = np.zeros((image_config.ny, image_config.nx, len(images)), dtype=np.uint8)
        bias_level_array = np.zeros(len(images))

        master_bias_filename = self.get_calibration_filename(image_config)
        logs.add_tag(logging_tags, 'master_bias', os.path.basename(master_bias_filename))
        for i, image in enumerate(images):
            bias_level_array[i] = stats.sigma_clipped_mean(image.data, 3.5, mask=image.bpm)

            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            logs.add_tag(logging_tags, 'bias', bias_level_array[i])
            self.logger.debug('Calculating bias level', extra=logging_tags)
            # Subtract the bias level for each image
            bias_data[:, :, i] = image.data[:, :] - bias_level_array[i]
            bias_mask[:, :, i] = image.bpm[:, :]

        mean_bias_level = stats.sigma_clipped_mean(bias_level_array, 3.0)

        master_bias = stats.sigma_clipped_mean(bias_data, 3.0, axis=2, mask=bias_mask)

        del bias_data
        del bias_mask

        master_bpm = np.array(master_bias == 0.0, dtype=np.uint8)

        header = fits_utils.create_master_calibration_header(images)

        header['BIASLVL'] = (mean_bias_level, 'Mean bias level of master bias')
        master_bias_image = Image(data=master_bias, header=header)
        master_bias_image.filename = master_bias_filename
        master_bias_image.bpm = master_bpm

        logs.pop_tag(logging_tags, 'master_bias')
        logs.add_tag(logging_tags, 'filename', os.path.basename(master_bias_image.filename))
        logs.add_tag(logging_tags, 'bias', mean_bias_level)
        self.logger.debug('Average bias level in ADU', extra=logging_tags)

        return [master_bias_image]


class BiasSubtractor(ApplyCalibration):
    def __init__(self, pipeline_context):
        super(BiasSubtractor, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return ['ccdsum']

    @property
    def calibration_type(self):
        return 'bias'

    def apply_master_calibration(self, images, master_calibration_image, logging_tags):

        master_bias_data = master_calibration_image.data
        master_bias_level = float(master_calibration_image.header['BIASLVL'])

        logs.add_tag(logging_tags, 'bias', master_bias_level)
        logs.add_tag(logging_tags, 'master_bias',
                     os.path.basename(master_calibration_image.filename))

        for image in images:
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            self.logger.info('Subtracting bias', extra=logging_tags)

            image.subtract(master_bias_level)
            image.subtract(master_bias_data)

            image.bpm |= master_calibration_image.bpm
            image.header['BIASLVL'] = (master_bias_level, 'Mean bias level of master bias')

            master_bias_filename = os.path.basename(master_calibration_image.filename)
            image.header['L1IDBIAS'] = (master_bias_filename, 'ID of bias frame used')
            image.header['L1STATBI'] = (1, "Status flag for bias frame correction")
            self.logger.info('Subtracting bias', extra=logging_tags)
        return images


class OverscanSubtractor(Stage):
    def __init__(self, pipeline_context):
        super(OverscanSubtractor, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):

        for image in images:
            logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))

            # Subtract the overscan if it exists
            if len(image.data.shape) > 2:
                for i in range(image.data.shape[0]):
                    _subtract_overscan_3d(image, i)
                    logs.add_tag(logging_tags, 'overscan', overscan_level)
                    logs.add_tag(logging_tags, 'quadrant', i + 1)
                    self.logger.info('Subtracting overscan', extra=logging_tags)
            else:
                overscan_level = _subtract_overscan_2d(image)
                logs.add_tag(logging_tags, 'overscan', overscan_level)
                self.logger.info('Subtracting overscan', extra=logging_tags)

        return images


def _subtract_overscan_3d(image, i):
    overscan_region = fits_utils.parse_region_keyword(image.header.get('BIASSEC{0}'.format(i + 1)))
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
