from __future__ import absolute_import, print_function, division

from pylcogt import logs
from pylcogt.utils import fits_utils
from pylcogt.stages import Stage

import os

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

    image.header['L1STATTR'] = (1, 'Status flag for overscan trimming')

    return image.header['NAXIS1'], image.header['NAXIS2']


class Trimmer(Stage):
    def __init__(self, pipeline_context):
        super(Trimmer, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):

        for image in images:

            logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            logs.add_tag(logging_tags, 'trimsec', image.header['TRIMSEC'])
            self.logger.info('Trimming image', extra=logging_tags)

            nx, ny = _trim_image(image)
            image.update_shape(nx, ny)
        return images
