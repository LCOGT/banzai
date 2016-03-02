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

        for i, image in enumerate(images):
            bias_level_array[i] = stats.sigma_clipped_mean(image.data, 3.5, mask=image.bpm)

            logs.add_tag(logging_tags, 'filename', image.filename)
            self.logger.debug('Bias level is {bias}'.format(bias=bias_level_array[i]),
                              extra=logging_tags)
            # Subtract the bias level for each image
            bias_data[:, :, i] = image.data - bias_level_array[i]
            bias_mask[:, :, i] = image.bpm

        logs.pop_tag(logging_tags, 'filename')
        mean_bias_level = stats.sigma_clipped_mean(bias_level_array, 3.0)
        self.logger.debug('Average bias level: {bias} ADU'.format(bias=mean_bias_level),
                          extra=logging_tags)

        master_bias = stats.sigma_clipped_mean(bias_data, 3.0, axis=2, mask=bias_mask)
        master_bpm = np.array(np.isnan(master_bias), dtype=np.uint8)
        master_bias[master_bpm] = 0.0

        header = fits_utils.create_master_calibration_header(images)

        header['BIASLVL'] = mean_bias_level
        master_bias_image = Image(data=master_bias, header=header)
        master_bias_image.filename = self.get_calibration_filename(image_config)
        master_bias_image.bpm = master_bpm
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

        for image in images:
            logs.add_tag(logging_tags, 'filename', image.filename)
            self.logger.info('Subtracting bias', extra=logging_tags)

            image.subtract(master_bias_level)
            image.subtract(master_bias_data)

            image.bpm |= master_calibration_image.bpm
            image.header['BIASLVL'] = master_bias_level

            master_bias_filename = os.path.basename(master_calibration_image.filename)
            image.header['L1IDBIAS'] = master_bias_filename

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
            logs.add_tag(logging_tags, 'filename', image.filename)
            self.logger.info('Subtracting overscan', extra=logging_tags)

            # Subtract the overscan if it exists
            overscan_region = fits_utils.parse_region_keyword(image.header.get('BIASSEC'))
            if overscan_region is not None:
                overscan_level = stats.sigma_clipped_mean(image.data[overscan_region], 3)
            else:
                overscan_level = 0.0

            image.subtract(overscan_level)
            image.header['OVERSCAN'] = overscan_level

        return images
