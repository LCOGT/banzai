from __future__ import absolute_import, print_function, division

import os.path

import numpy as np
from astropy.io import fits

from pylcogt.images import Image, check_image_homogeneity
from pylcogt.utils import date_utils
from . import logs
from .stages import CalibrationMaker, ApplyCalibration, Stage
from .utils import stats, fits_utils


__author__ = 'cmccully'


def create_master_bias_header(images, mean_bias_level, mean_date_obs):
    header = fits.Header()
    for h in images[0].header.keys():
        header[h] = images[0].header[h]

    header['BIASLVL'] = mean_bias_level
    header['DATE-OBS'] = date_utils.date_obs_to_string(mean_date_obs)

    header.add_history("Images combined to create master bias image:")
    for image in images:
        header.add_history(image.filename)
    return header


class BiasMaker(CalibrationMaker):
    min_images = 5

    def __init__(self, pipeline_context):
        super(BiasMaker, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return ['ccdsum']

    @property
    def calibration_type(self):
        return 'BIAS'

    def do_stage(self, images):
        if len(images) < self.min_images:
            # Do nothing
            self.logger.warning('Not enough images to combine.')
            return []
        else:

            image_config = check_image_homogeneity(images)
            logging_tags = logs.image_config_to_tags(image_config, self.group_by_keywords)

            bias_data = np.zeros((image_config.ny, image_config.nx, len(images)))

            bias_level_array = np.zeros(len(images))

            for i, image in enumerate(images):
                bias_level_array[i] = stats.sigma_clipped_mean(image.data, 3.5)

                logs.add_tag(logging_tags, 'filename', image.filename)
                self.logger.debug('Bias level is {bias}'.format(bias=bias_level_array[i]),
                                  extra=logging_tags)
                # Subtract the bias level for each image
                bias_data[:, :, i] = image.data - bias_level_array[i]

            logs.pop_tag(logging_tags, 'filename')
            mean_bias_level = stats.sigma_clipped_mean(bias_level_array, 3.0)
            self.logger.debug('Average bias level: {bias} ADU'.format(bias=mean_bias_level),
                              extra=logging_tags)

            master_bias = stats.sigma_clipped_mean(bias_data, 3.0, axis=2)

            observation_dates = [image.dateobs for image in images]
            mean_dateobs = date_utils.mean_date(observation_dates)

            header = create_master_bias_header(images, mean_bias_level, mean_dateobs)

            master_bias_image = Image(data=master_bias, header=header)
            master_bias_image.filename = self.get_calibration_filename(image_config)

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

    def do_stage(self, images):

        if len(images) == 0:
            # Abort!
            return []
        else:

            image_config = check_image_homogeneity(images)
            logging_tags = logs.image_config_to_tags(image_config, self.group_by_keywords)

            master_bias_filename = self.get_calibration_filename(images[0])
            master_bias_image = Image(master_bias_filename)
            master_bias_data = master_bias_image.data
            master_bias_level = float(master_bias_image.header['BIASLVL'])

            for image in images:
                logs.add_tag(logging_tags, 'filename', image.filename)
                self.logger.info('Subtracting bias', extra=logging_tags)

                image.subtract(master_bias_level)
                image.subtract(master_bias_data)

                image.header['BIASLVL'] = master_bias_level

                master_bias_filename = os.path.basename(master_bias_filename)
                image.add_history('Master Bias: {bias_file}'.format(bias_file=master_bias_filename))

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
