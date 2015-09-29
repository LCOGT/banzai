from __future__ import absolute_import, print_function
from .stages import Stage
from . import dbs
from .utils import fits_utils
from astropy.io import fits
__author__ = 'cmccully'

class Trim(Stage):
    def __init__(self, initial_query, processed_path):

        trim_query = initial_query & (dbs.Image.obstype.in_(('DARK', 'SKYFLAT', 'EXPOSE')))

        super(Trim, self).__init__(self.trim, processed_path=processed_path,
                                   initial_query=trim_query, logger_name='Trim', cal_type='trim')
        self.log_message = 'Trimming {image_name} to {trim_region}.'
        self.group_by = []

    def trim(self, image_files, output_files, clobber=True):
        for i, image in enumerate(image_files):
            hdu = fits.open(image)
            trimsec = fits_utils.parse_region_keyword(hdu[0].header['TRIMSEC'])
            hdu[0].data = hdu[0].data[trimsec]
            # Update the NAXIS and CRPIX keywords
            hdu[0].header['NAXIS1'] = trimsec[1].stop - trimsec[1].start
            hdu[0].header['NAXIS2'] = trimsec[0].stop - trimsec[0].start
            hdu[0].header['CRPIX1'] -= trimsec[1].start
            hdu[0].header['CRPIX2'] -= trimsec[0].start
            hdu.writeto(output_files[i], clobber=clobber)
