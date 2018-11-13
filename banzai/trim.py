import logging

from banzai.utils import fits_utils
from banzai.stages import Stage

logger = logging.getLogger(__name__)


def _trim_image(image):
    trimsec = fits_utils.parse_region_keyword(image.header['TRIMSEC'])

    if trimsec is not None:
        image.data = image.data[trimsec]
        image.bpm = image.bpm[trimsec]

        # Update the NAXIS and CRPIX keywords
        image.header['NAXIS1'] = trimsec[1].stop - trimsec[1].start
        image.header['NAXIS2'] = trimsec[0].stop - trimsec[0].start
        if 'CRPIX1' in image.header:
            image.header['CRPIX1'] -= trimsec[1].start
        if 'CRPIX2' in image.header:
            image.header['CRPIX2'] -= trimsec[0].start

        image.header['L1STATTR'] = (1, 'Status flag for overscan trimming')
    else:
        logger.warning('TRIMSEC was not defined.', image=image, extra_tags={'trimsec': image.header['TRIMSEC']})
        image.header['L1STATTR'] = (0, 'Status flag for overscan trimming')
    return image.header['NAXIS1'], image.header['NAXIS2']


class Trimmer(Stage):
    def __init__(self, pipeline_context):
        super(Trimmer, self).__init__(pipeline_context)

    def do_stage(self, images):

        for image in images:

            logger.info('Trimming image', image=image, extra_tags={'trimsec': image.header['TRIMSEC']})

            nx, ny = _trim_image(image)
            image.update_shape(nx, ny)
        return images
