from __future__ import absolute_import, print_function, division

from astropy.io import fits
import numpy as np
import os.path

from .utils import stats, fits_utils
from . import dbs
from . import logs
from .stages import MakeCalibrationImage, ApplyCalibration

__author__ = 'cmccully'


class MakeBias(MakeCalibrationImage):
    def __init__(self, raw_path, processed_path, initial_query):

        super(MakeBias, self).__init__(self.make_master_bias, processed_path=processed_path,
                                       initial_query=initial_query, logger_name='Bias',
                                       cal_type='bias', previous_suffix_number='03',
                                       previous_stage_done=dbs.Image.ingest_done)
        self.log_message = 'Creating {binning} bias frame for {instrument} on {epoch}.'
        self.group_by = [dbs.Image.ccdsum]

    def make_master_bias(self, image_list, output_file, min_images=5, clobber=True):

        logger = logs.get_logger('Bias')
        if len(image_list) < min_images:
            logger.warning('Not enough images to combine.')
        else:
            # Assume the files are all the same number of pixels
            # TODO: add error checking for incorrectly sized images

            nx = image_list[0].naxis1
            ny = image_list[0].naxis2
            bias_data = np.zeros((ny, nx, len(image_list)))

            bias_level_array = np.zeros(len(image_list))
            read_noise_array = np.zeros(len(image_list))

            for i, image in enumerate(image_list):
                image_file = os.path.join(image.filepath, image.filename)
                image_file += self.previous_image_suffix + '.fits'
                image_data = fits.getdata(image_file)
                bias_level_array[i] = stats.sigma_clipped_mean(image_data, 3.5)

                logger.debug('Bias level for {file} is {bias}'.format(file=image.filename,
                                                                      bias=bias_level_array[i]))
                # Subtract the bias level for each image
                bias_data[:, :, i] = image_data - bias_level_array[i]

            mean_bias_level = stats.sigma_clipped_mean(bias_level_array, 3.0)
            logger.info('Average bias level: {bias} ADU'.format(bias=mean_bias_level))

            master_bias = stats.sigma_clipped_mean(bias_data, 3.0, axis=2)

            for i, image in enumerate(image_list):
                # Estimate the read noise for each image
                read_noise = stats.robust_standard_deviation(bias_data[:, :, i] - master_bias)

                # Make sure to convert to electrons and save
                read_noise_array[i] = read_noise * image.gain
                log_message = 'Read noise estimate for {file} is {rdnoise}'
                logger.debug(log_message.format(file=image.filename, rdnoise=read_noise_array[i]))

            mean_read_noise = stats.sigma_clipped_mean(read_noise_array, 3.0)
            logger.info('Estimated Readnoise: {rdnoise} e-'.format(rdnoise=mean_read_noise))
            # Save the master bias image with all of the combined images in the header

            header = fits.Header()
            header['CCDSUM'] = image_list[0].ccdsum
            header['DAY-OBS'] = str(image_list[0].dayobs)
            header['CALTYPE'] = 'BIAS'
            header['BIASLVL'] = mean_bias_level
            header['RDNOISE'] = mean_read_noise

            header.add_history("Images combined to create master bias image:")
            for image in image_list:
                header.add_history(image.filename)

            fits.writeto(output_file, master_bias, header=header, clobber=clobber)

            self.save_calibration_info('bias', output_file, image_list[0])


class SubtractBias(ApplyCalibration):
    def __init__(self, raw_path, processed_path, initial_query):

        bias_query = initial_query & (dbs.Image.obstype.in_(('DARK', 'SKYFLAT', 'EXPOSE')))

        super(SubtractBias, self).__init__(self.subtract_bias, processed_path=processed_path,
                                           initial_query=bias_query, logger_name='Bias', cal_type='bias',
                                           image_suffix_number='10', previous_suffix_number='03',
                                           previous_stage_done=dbs.Image.ingest_done)
        self.log_message = 'Subtracting bias frame'
        self.group_by = [dbs.Image.ccdsum]

    def subtract_bias(self, image_files, output_files, master_bias_file, clobber=True):

        master_bias_data = fits.getdata(master_bias_file)
        master_bias_level = float(fits.getval(master_bias_file, 'BIASLVL'))

        logger = logs.get_logger('Bias')
        db_session = dbs.get_session()
        # TODO Add error checking for incorrect image sizes
        for i, image in enumerate(image_files):
            telescope = db_session.query(dbs.Telescope).filter(dbs.Telescope.id == image.telescope_id).one()
            tags = logs.image_config_to_tags(image, telescope, image.dayobs, self.group_by)
            tags['tags']['filename'] = image.filename
            tags['tags']['obstype'] = image.obstype
            logger.debug('Subtracting bias for {image}'.format(image=image.filename), extra=tags)

            image_file = os.path.join(image.filepath, image.filename)
            image_file += self.previous_image_suffix + '.fits'
            data = fits.getdata(image_file)
            header = fits_utils.sanitizeheader(fits.getheader(image_file))

            # Subtract the overscan first if it exists
            overscan_region = fits_utils.parse_region_keyword(header['BIASSEC'])
            if overscan_region is not None:
                bias_level = stats.sigma_clipped_mean(data[overscan_region], 3)
            else:
                # If not, subtract the master bias level
                bias_level = master_bias_level

            logger.debug('Bias level: {bias}'.format(bias=bias_level), extra=tags)
            data -= bias_level
            data -= master_bias_data

            header['BIASLVL'] = bias_level

            master_bias_filename = os.path.basename(master_bias_file)
            header.add_history('Master Bias: {bias_file}'.format(bias_file=master_bias_filename))
            output_filename = os.path.join(output_files[i].filepath, output_files[i].filename)
            output_filename += self.image_suffix_number + '.fits'
            fits.writeto(output_filename, data, header=header, clobber=clobber)
            image.bias_done = True
            db_session.add(image)
            db_session.commit()

        db_session.close()