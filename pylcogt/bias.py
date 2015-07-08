from __future__ import absolute_import, print_function
__author__ = 'cmccully'

from astropy.io import fits
import numpy as np
import os.path

from .utils.stats import robust_standard_deviation
from . import dbs

def make_master_bias(image_list, output_filename, minimages=5, clobber=True):

    if len(image_list) >= minimages:

        # Assume the files are all the same number of pixels, should add error checking
        nx = fits.getval(image_list[0], 'NAXIS1')
        ny = fits.getval(image_list[0], 'NAXIS2')
        bias_data = np.zeros((ny, nx, len(image_list)))

        read_noise_array = np.zeros(len(image_list))
        bias_level_array = np.zeros(len(image_list))

        for i, image in enumerate(image_list):
            image_data = fits.getdata(image)
            # Do a robust Standard deviation on each individual image to estimate the read noise
            read_noise = robust_standard_deviation(image_data)

            # Make sure to convert to electrons and save
            read_noise_array[i] = read_noise * float(fits.getval(image, 'GAIN'))

            # Throw away any pixels that are 3.5 sigma from the median
            image_deviation = np.abs(image_data - np.median(image_data))
            image_mask = image_deviation > (3.5 * read_noise)

            # Take the sigma clipped mean to estimate the bias level of each individual image
            bias_level_array[i] = np.mean(image_data[~image_mask])

            # Subtract the bias level for each image
            bias_data[:, :, i]  = image_data - bias_level_array[i]

        # Take the robust standard deviation pixel by pixel
        bias_standard_deviation = np.expand_dims(robust_standard_deviation(bias_data, axis=2), axis=2)

        # Throw away any pixels that are more than 3 sigma away from the median
        bias_absolute_deviation = np.abs(bias_data - np.expand_dims(np.median(bias_data, axis=2), axis=2))
        bias_mask = bias_absolute_deviation > (3.0 * bias_standard_deviation)

        # Take the sigma clipped mean pixel by pixel to make the master bias image
        bias_data[bias_mask] = 0.0

        master_bias = bias_data.sum(axis=2)
        master_bias /= np.logical_not(bias_mask).sum(axis=2)

        # Save the master bias image with all of the combined images in the header
        ccdsum = fits.getval(image_list[0], 'CCDSUM')
        dayobs = fits.getval(image_list[0], 'DAY-OBS')

        header = fits.Header()
        header['CCDSUM'] = ccdsum
        header['DAY-OBS'] = dayobs
        header['CALTYPE'] = 'BIAS'
        header['BIASLVL'] = bias_level_array.mean()
        header['RDNOISE'] = read_noise_array.mean()

        header.add_history("Images combined to create master bias image:")
        for image in image_list:
            header.add_history(image)

        fits.writeto(output_filename, master_bias, header=header, clobber=clobber)

        # Store the information into the calibration table
        calibration_image = dbs.Calibration_Image()
        calibration_image.dayobs = dayobs
        calibration_image.ccdsum = ccdsum
        calibration_image.type = 'BIAS'
        calibration_image.filename = os.path.basename(output_filename)
        calibration_image.filepath = os.path.dirname(output_filename)

        db_session = dbs.get_session()
        db_session.add(calibration_image)
        db_session.commit()
        db_session.close()
