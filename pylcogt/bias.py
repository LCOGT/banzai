from __future__ import absolute_import, print_function
__author__ = 'cmccully'

from astropy.io import fits
import numpy as np
import os.path

from .utils import stats
from . import dbs
from . import logs

def run_make_bias(telescope, epoch, image_query, processed_path):
    db_session = dbs.get_session()

    # Select only bias images
    bias_query = image_query & (dbs.Image.telescope_id == telescope.id)
    bias_query = bias_query & (dbs.Image.dayobs == epoch)
    bias_query = bias_query & (dbs.Image.obstype == 'BIAS')

    # Get the distinct values of ccdsum that we are making bias frames for.
    ccdsum_list = db_session.query(dbs.Image.ccdsum).filter(bias_query).distinct()

    logger = logs.get_logger('Bias')

    for image_config in ccdsum_list:
        log_message = 'Creating {binning} bias frame for {instrument} on {epoch}'
        log_message = log_message.format(binning=image_config.ccdsum.replace(' ','x'),
                                         instrument=telescope.instrument, epoch=epoch)
        logger.info(log_message)

        # Select only images with the correct binning
        bias_ccdsum_query = bias_query & (dbs.Image.ccdsum == image_config.ccdsum)
        bias_ccdsum_list = db_session.query(dbs.Image).filter(bias_ccdsum_query).all()

        # Convert from image objects to file names
        bias_image_list = []
        for image in bias_ccdsum_list:
            bias_image_list.append(os.path.join(image.rawpath, image.rawfilename))

        # Create output directory if necessary
        output_directory = os.path.join(processed_path, telescope.site,
                                        telescope.instrument, epoch)
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)


        master_bias_file = '{filepath}/bias_{instrument}_{epoch}_bin{bin}.fits'
        master_bias_file = master_bias_file.format(filepath=output_directory,
                                                   instrument=telescope.instrument, epoch=epoch,
                                                   bin=image_config.ccdsum.replace(' ','x'))

        make_master_bias(bias_image_list, master_bias_file)
    db_session.close()


def make_master_bias(image_list, output_file, min_images=5, clobber=True):

    if len(image_list) >= min_images:
        logger = logs.get_logger('Bias')

        # Assume the files are all the same number of pixels
        # TODO: add error checking for incorrectly sized images
        nx = fits.getval(image_list[0], 'NAXIS1')
        ny = fits.getval(image_list[0], 'NAXIS2')
        bias_data = np.zeros((ny, nx, len(image_list)))

        bias_level_array = np.zeros(len(image_list))

        for i, image in enumerate(image_list):
            image_data = fits.getdata(image)
            bias_level_array[i] = stats.sigma_clipped_mean(image_data, 3.5)

            logger.debug('Bias level for {file} is {bias}'.format(file=os.path.basename(image),
                                                                  bias=bias_level_array[i]))
            # Subtract the bias level for each image
            bias_data[:, :, i]  = image_data - bias_level_array[i]

        mean_bias_level = bias_level_array.mean()
        logger.info('Average bias level: {bias} ADU'.format(bias=mean_bias_level))

        master_bias = stats.sigma_clipped_mean(bias_data, 3.0, axis=2)

        # Estimate the read noise for each image
        read_noise_array = np.zeros(len(image_list))
        for i, image in enumerate(image_list):
            image_data = fits.getdata(image)
            # Make sure to convert to electrons and save
            read_noise = stats.robust_standard_deviation(image_data - master_bias)
            read_noise_array[i] = read_noise * float(fits.getval(image, 'GAIN'))
            log_message = 'Read noise estimate for {file} is {rdnoise}'
            logger.debug(log_message.format(file=os.path.basename(image), rdnoise=read_noise))

        mean_read_noise = read_noise_array.mean()
        logger.info('Estimated Readnoise: {rdnoise} e-'.format(rdnoise=mean_read_noise))
        # Save the master bias image with all of the combined images in the header
        ccdsum = fits.getval(image_list[0], 'CCDSUM')
        dayobs = fits.getval(image_list[0], 'DAY-OBS')

        header = fits.Header()
        header['CCDSUM'] = ccdsum
        header['DAY-OBS'] = dayobs
        header['CALTYPE'] = 'BIAS'
        header['BIASLVL'] = bias_level_array.mean()
        header['RDNOISE'] = mean_read_noise

        header.add_history("Images combined to create master bias image:")
        for image in image_list:
            header.add_history(os.path.basename(image))

        fits.writeto(output_file, master_bias, header=header, clobber=clobber)

        # Store the information into the calibration table
        db_session = dbs.get_session()
        # Check and see if the bias file is already in the database
        image_query = db_session.query(dbs.Calibration_Image)
        output_filename = os.path.basename(output_file)
        image_query = image_query.filter(dbs.Calibration_Image.filename == output_filename)
        image_query = image_query.all()

        if len(image_query) == 0:
            # Create a new row
            calibration_image = dbs.Calibration_Image()
        else:
            # Otherwise update the existing data
            # In principle we could just skip this, but this should be fast
            calibration_image = image_query[0]

        calibration_image.dayobs = dayobs
        calibration_image.ccdsum = ccdsum
        calibration_image.type = 'BIAS'
        calibration_image.filename = output_filename
        calibration_image.filepath = os.path.dirname(output_file)

        db_session.add(calibration_image)
        db_session.commit()
        db_session.close()
