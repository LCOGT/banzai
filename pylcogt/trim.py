from __future__ import absolute_import, print_function, division
from .stages import Stage
from . import dbs, logs
from .utils import fits_utils
from astropy.io import fits
import os

__author__ = 'cmccully'

class Trim(Stage):
    def __init__(self, raw_path, processed_path, initial_query):

        trim_query = initial_query & (dbs.Image.obstype.in_(('DARK', 'SKYFLAT', 'EXPOSE')))

        super(Trim, self).__init__(self.trim, processed_path=processed_path,
                                   initial_query=trim_query, logger_name='Trim', cal_type='trim')
        self.log_message = 'Trimming images from {instrument} at {site} on {epoch}.'
        self.group_by = None

    def get_output_images(self, telescope, epoch):
        image_sets, image_configs = self.select_input_images(telescope, epoch)
        return [image for image_set in image_sets for image in image_set]

    def trim(self, image_files, output_files, clobber=True):
        logger = logs.get_logger('Trim')
        for i, image in enumerate(image_files):
            image_file = os.path.join(image.filepath, image.filename)
            hdu = fits.open(image_file)

            trimsec = fits_utils.parse_region_keyword(hdu[0].header['TRIMSEC'])
            logger.debug('Trimming {image_name} to {trim_sec}'.format(image_name=image.filename,
                                                                      trim_sec=hdu[0].header['TRIMSEC']))
            hdu[0].data = hdu[0].data[trimsec]
            # Update the NAXIS and CRPIX keywords
            hdu[0].header['NAXIS1'] = trimsec[1].stop - trimsec[1].start
            hdu[0].header['NAXIS2'] = trimsec[0].stop - trimsec[0].start
            hdu[0].header['CRPIX1'] -= trimsec[1].start
            hdu[0].header['CRPIX2'] -= trimsec[0].start

            # Update the database
            image.naxis1 = hdu[0].header['NAXIS1']
            image.naxis2 = hdu[0].header['NAXIS2']
            output_filename = os.path.join(output_files[i].filepath, output_files[i].filename)
            hdu.writeto(output_filename, clobber=clobber)
        self.db_session.flush()