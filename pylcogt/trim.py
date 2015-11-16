from __future__ import absolute_import, print_function, division
from .stages import Stage
from . import dbs, logs
from .utils import fits_utils
from astropy.io import fits
from itertools import izip

__author__ = 'cmccully'

logger = logs.get_logger('Trim')


def _trim_image(hdu):
    trimsec = fits_utils.parse_region_keyword(hdu[0].header['TRIMSEC'])

    hdu[0].data = hdu[0].data[trimsec]
    # Update the NAXIS and CRPIX keywords
    hdu[0].header['NAXIS1'] = trimsec[1].stop - trimsec[1].start
    hdu[0].header['NAXIS2'] = trimsec[0].stop - trimsec[0].start
    hdu[0].header['CRPIX1'] -= trimsec[1].start
    hdu[0].header['CRPIX2'] -= trimsec[0].start

    return hdu[0].header['NAXIS1'], hdu[0].header['NAXIS2']


class Trim(Stage):
    def __init__(self, raw_path, processed_path, initial_query, cpu_pool):

        trim_query = initial_query & (dbs.Image.obstype.in_(('DARK', 'SKYFLAT', 'EXPOSE')))

        super(Trim, self).__init__(self.trim, processed_path=processed_path,
                                   initial_query=trim_query, logger_name='Trim', cal_type='trim',
                                   previous_stage_done=dbs.Image.bias_done, previous_suffix_number='10',
                                   image_suffix_number='15', cpu_pool=cpu_pool)
        self.log_message = 'Trimming images from {instrument} at {site} on {epoch}.'
        self.group_by = None

    def get_output_images(self, telescope, epoch):
        return self.select_input_images(telescope, epoch)[0][0]

    def trim(self, input_images, output_images):

        images_to_save = []
        for input_image, output_image in izip(input_images, output_images):
            image_file = input_image.get_full_filename(self.previous_image_suffix)

            hdu = fits.open(image_file)

            naxis1, naxis2 = _trim_image(hdu)

            logger.debug('Trimming {image_name} to {trim_sec}'.format(image_name=input_image.filename,
                                                                      trim_sec=hdu[0].header['TRIMSEC']))

            output_filename = output_image.get_full_filename(self.image_suffix_number)
            hdu.writeto(output_filename, clobber=True)

            # Update the database
            input_image.naxis1 = naxis1
            input_image.naxis2 = naxis2
            input_image.trim_done = True

            images_to_save.append(input_image)

        dbs.save_images(images_to_save)
