from __future__ import absolute_import, print_function, division

from astropy.io import fits
import numpy as np
import os.path

from .utils import stats, fits_utils
from pylcogt import dbs
from . import logs
from pylcogt.utils import date_utils
from .stages import CalibrationMaker, ApplyCalibration
from pylcogt.utils.images import Image
from pylcogt.utils import file_utils

__author__ = 'cmccully'


def check_image_homogeneity(images):
    for attribute in ('nx', 'ny', 'ccdsum', 'epoch'):
        if len({getattr(image, attribute) for image in images}) > 1:
            raise InhomogeneousSetException('Images have different {}s'.format(attribute))
    return images[0]


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
            self.logger.info('Running {0}'.format(self.stage_name), extra=tags)
            bias_data = np.zeros((image_config.ny, image_config.nx, len(images)))

            bias_level_array = np.zeros(len(images))

            for i, image in enumerate(images):
                bias_level_array[i] = stats.sigma_clipped_mean(image.data, 3.5)

                logging_tags['filename'] = image.filename
                self.logger.debug('Bias level is {bias}'.format(bias=bias_level_array[i]),
                                  extra=logging_tags)
                # Subtract the bias level for each image
                bias_data[:, :, i] = image.subtract(bias_level_array[i])

            logging_tags.pop('filename')
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


class InhomogeneousSetException(Exception):
    pass


def estimate_readnoise(images):
    read_noise_array = np.zeros(len(images))
    for i, image in enumerate(images):
        # Estimate the read noise for each image
        read_noise = stats.robust_standard_deviation(bias_data[:, :, i] - master_bias)

        # Make sure to convert to electrons and save
        read_noise_array[i] = read_noise * image.gain
        log_message = 'Read noise estimate for {file} is {rdnoise}'
        logger.debug(log_message.format(file=image.filename, rdnoise=read_noise_array[i]))

    mean_read_noise = stats.sigma_clipped_mean(read_noise_array, 3.0)
    logger.info('Estimated Readnoise: {rdnoise} e-'.format(rdnoise=mean_read_noise))
    # Save the master bias image with all of the combined images in the header
    header['RDNOISE'] = mean_read_noise


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
            master_bias_filename = self.get_calibration_filename(images[0])
            master_bias_image = Image(master_bias_filename)
            master_bias_data = master_bias_image.data
            master_bias_level = float(master_bias_image.header['BIASLVL'])

            # TODO Add error checking for incorrect image sizes
            for image in images:
                self.logger.debug('Subtracting bias for {image}'.format(image=image.filename))

                # Subtract the overscan first if it exists
                overscan_region = fits_utils.parse_region_keyword(image.header.get('BIASSEC'))
                if overscan_region is not None:
                    bias_level = stats.sigma_clipped_mean(image[0].data[overscan_region], 3)
                else:
                    # If not, subtract the master bias level
                    bias_level = master_bias_level

                self.logger.debug('Bias level: {bias}'.format(bias=bias_level))

                image.subtract(bias_level)
                image.subtract(master_bias_data)

                image.header['BIASLVL'] = bias_level

                master_bias_filename = os.path.basename(master_bias_filename)
                image.header.add_history('Master Bias: {bias_file}'.format(bias_file=master_bias_filename))

        return images