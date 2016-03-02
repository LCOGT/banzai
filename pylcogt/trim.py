from __future__ import absolute_import, print_function, division

import itertools

from astropy.io import fits

from . import dbs, logs
from .utils import fits_utils
from pylcogt.stages import make_output_directory, Stage

__author__ = 'cmccully'


def _trim_image(image):
    trimsec = fits_utils.parse_region_keyword(image.header['TRIMSEC'])

    image.data = image.data[trimsec]
    image.bpm = image.bpm[trimsec]
    
    # Update the NAXIS and CRPIX keywords
    image.header['NAXIS1'] = trimsec[1].stop - trimsec[1].start
    image.header['NAXIS2'] = trimsec[0].stop - trimsec[0].start
    image.header['CRPIX1'] -= trimsec[1].start
    image.header['CRPIX2'] -= trimsec[0].start

    return image.header['NAXIS1'], image.header['NAXIS2']


class Trimmer(Stage):
    def __init__(self, pipeline_context):
        super(Trimmer, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):

        for image in images:

            self.logger.debug('Trimming {image_name} to {trim_sec}'.format(image_name=image.filename,
                                                                      trim_sec=image.header['TRIMSEC']))
            nx, ny = _trim_image(image)
            image.update_shape(nx, ny)
        return images