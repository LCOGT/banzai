import logging

import numpy as np

from banzai.stages import Stage
from banzai.utils import fits_utils

logger = logging.getLogger(__name__)


class MosaicCreator(Stage):
    def __init__(self, pipeline_context):
        super(MosaicCreator, self).__init__(pipeline_context)

    def do_stage(self, images):
        for image in images:
            if image.data_is_3d():
                logging_tags = {}
                nx, ny = get_mosaic_size(image, image.get_n_amps())
                mosaiced_data = np.zeros((ny, nx), dtype=np.float32)
                mosaiced_bpm = np.zeros((ny, nx), dtype=np.uint8)
                for i in range(image.get_n_amps()):
                    datasec = image.extension_headers[i]['DATASEC']
                    amp_slice = fits_utils.parse_region_keyword(datasec)
                    logging_tags['DATASEC{0}'.format(i + 1)] = datasec

                    detsec = image.extension_headers[i]['DETSEC']
                    mosaic_slice = fits_utils.parse_region_keyword(detsec)
                    logging_tags['DATASEC{0}'.format(i + 1)] = detsec

                    mosaiced_data[mosaic_slice] = image.data[i][amp_slice]
                    mosaiced_bpm[mosaic_slice] = image.bpm[i][amp_slice]

                image.data = mosaiced_data
                image.bpm = mosaiced_bpm
                # Flag any missing data
                image.bpm[image.data == 0.0] = 1
                image.update_shape(nx, ny)
                update_naxis_keywords(image, nx, ny)
                logger.info('Mosaiced image', image=image, extra_tags=logging_tags)
        return images


def update_naxis_keywords(image, nx, ny):
    if 'NAXIS3' in image.header.keys():
        image.header.pop('NAXIS3')
    image.header['NAXIS'] = 2
    image.header['NAXIS1'] = nx
    image.header['NAXIS2'] = ny


def get_mosaic_size(image, n_amps):
    x_pixel_limits = []
    y_pixel_limits = []
    for i in range(n_amps):
        detsec_keyword = image.extension_headers[i]['DETSEC']
        detsec = fits_utils.parse_region_keyword(detsec_keyword)
        if detsec is not None:
            x_pixel_limits.append(detsec[1].start + 1)
            x_pixel_limits.append(detsec[1].stop)
            y_pixel_limits.append(detsec[0].start + 1)
            y_pixel_limits.append(detsec[0].stop)
        else:
            x_pixel_limits.append(None)
            y_pixel_limits.append(None)

    # Clean out any Nones
    x_pixel_limits = [x if x is not None else 1 for x in x_pixel_limits]
    y_pixel_limits = [y if y is not None else 1 for y in y_pixel_limits]

    nx = np.max(x_pixel_limits) - np.min(x_pixel_limits) + 1
    ny = np.max(y_pixel_limits) - np.min(y_pixel_limits) + 1
    return nx, ny
