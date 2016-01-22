from __future__ import absolute_import, print_function, division

from astropy.io import fits
import numpy as np
import os.path

from .utils import stats, fits_utils
from . import dbs
from . import logs
from .stages import CalibrationMaker, ApplyCalibration

__author__ = 'cmccully'

class BiasMaker(CalibrationMaker):
    min_images = 5

    def __init__(self, pipeline_context):
        super(BiasMaker, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return ['ccdsum']

    def do_stage(self, images):
        if len(images) < self.min_images:
            # Do nothing
            self.logger.warning('Not enough images to combine.')
        else:

            bias_data = np.zeros((images[0].ny, images[0].nx, len(images)))

            bias_level_array = np.zeros(len(images))

            for i, image in enumerate(images):
                bias_level_array[i] = stats.sigma_clipped_mean(image.data, 3.5)

                self.logger.debug('Bias level for {file} is {bias}'.format(file=image.filename,
                                                                      bias=bias_level_array[i]))
                # Subtract the bias level for each image
                bias_data[:, :, i] = image.data - bias_level_array[i]

                bias_data[:, :, i] = image.remove_bias()
                def create_bias_array(self, sigma=3.5):
                    self.bias = stats.sigma_clipped_mean(self.data, sigma)
                def remove_bias(self):
                    return self.data - self.bias


            mean_bias_level = stats.sigma_clipped_mean(bias_level_array, 3.0)
            self.logger.debug('Average bias level: {bias} ADU'.format(bias=mean_bias_level))

            master_bias = stats.sigma_clipped_mean(bias_data, 3.0, axis=2)

            header = fits.Header()
            header['CCDSUM'] = images[0].ccdsum
            header['DAY-OBS'] = str(images[0].epoch)
            header['CALTYPE'] = 'BIAS'
            header['BIASLVL'] = mean_bias_level


            header.add_history("Images combined to create master bias image:")
            for image in images:
                header.add_history(image.filename)

            master_bias_filename = self.get_calibration_filename(images[0])
            fits.writeto(master_bias_filename, master_bias, header=header, clobber=True)

            self.save_calibration_info('bias', master_bias_filename, images[0])
        return images


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

class SubtractBias(ApplyCalibration):
    def __init__(self, pipeline_context):
        super(SubtractBias, self).__init__(pipeline_context)
        self.group_by = [dbs.Image.ccdsum]

    def do_stage(self, images):

        master_bias_file = self.get_calibration_image()
        master_bias_data = fits.getdata(master_bias_file)
        master_bias_level = float(fits.getval(master_bias_file, 'BIASLVL'))

        db_session = dbs.get_session()
        # TODO Add error checking for incorrect image sizes
        for image in images:
            telescope = db_session.query(dbs.Telescope).filter(dbs.Telescope.id == image.telescope_id).one()
            tags = logs.image_config_to_tags(image, telescope, image.dayobs, self.group_by)
            self.logger.debug('Subtracting bias for {image}'.format(image=image.filename), extra=tags)


            # Subtract the overscan first if it exists
            overscan_region = fits_utils.parse_region_keyword(image[0].header.get('BIASSEC'))
            if overscan_region is not None:
                bias_level = stats.sigma_clipped_mean(image[0].data[overscan_region], 3)
            else:
                # If not, subtract the master bias level
                bias_level = master_bias_level

            self.logger.debug('Bias level: {bias}'.format(bias=bias_level), extra=tags)
            image[0].data -= bias_level
            image[0].data -= master_bias_data

            image[0].header['BIASLVL'] = bias_level

            master_bias_filename = os.path.basename(master_bias_file)
            image[0].header.add_history('Master Bias: {bias_file}'.format(bias_file=master_bias_filename))
