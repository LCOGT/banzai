import logging

import numpy as np

from banzai.stages import Stage
from banzai.utils import fits_utils

logger = logging.getLogger('banzai')


class MosaicCreator(Stage):
    def __init__(self, runtime_context):
        super(MosaicCreator, self).__init__(runtime_context)

    def do_stage(self, image):
        if image.data_is_3d():
            logging_tags = {}
            nx, ny = get_mosaic_size(image, image.get_n_amps())
            mosaiced_data = np.zeros((ny, nx), dtype=np.float32)
            mosaiced_bpm = np.zeros((ny, nx), dtype=np.uint8)
            x_detsec_limits, y_detsec_limits = get_detsec_limits(image, image.get_n_amps())
            xmin = min(x_detsec_limits) - 1
            ymin = min(y_detsec_limits) - 1
            for i in range(image.get_n_amps()):
                ccdsum = image.extension_headers[i].get('CCDSUM', image.ccdsum)
                x_binning, y_binning = ccdsum.split(' ')
                datasec = image.extension_headers[i]['DATASEC']
                amp_slice = fits_utils.parse_region_keyword(datasec)
                logging_tags['DATASEC{0}'.format(i + 1)] = datasec

                detsec = image.extension_headers[i]['DETSEC']
                mosaic_slice = get_windowed_mosaic_slices(detsec, xmin, ymin, x_binning, y_binning)

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
        return image


def get_windowed_mosaic_slices(detsec, xmin, ymin, x_binning, y_binning):
    unbinned_slice = fits_utils.parse_region_keyword(detsec)
    return (slice((unbinned_slice[0].start - ymin) // int(y_binning),
                  (unbinned_slice[0].stop - ymin) // int(y_binning),
                  unbinned_slice[0].step),
            slice((unbinned_slice[1].start - xmin) // int(x_binning),
                  (unbinned_slice[1].stop - xmin) // int(x_binning),
                  unbinned_slice[1].step))


def update_naxis_keywords(image, nx, ny):
    if 'NAXIS3' in image.header.keys():
        image.header.pop('NAXIS3')
    image.header['NAXIS'] = 2
    image.header['NAXIS1'] = nx
    image.header['NAXIS2'] = ny


def get_detsec_limits(image, n_amps):
    x_pixel_limits = []
    y_pixel_limits = []
    for i in range(n_amps):
        detsec_keyword = image.extension_headers[i]['DETSEC']
        detsec = fits_utils.parse_region_keyword(detsec_keyword)
        if detsec is not None:
            # Convert starts from 0 indexed to 1 indexed
            x_pixel_limits.append(detsec[1].start + 1)
            y_pixel_limits.append(detsec[0].start + 1)
            # Note python is not inclusive at the end (unlike IRAF)
            x_pixel_limits.append(detsec[1].stop)
            y_pixel_limits.append(detsec[0].stop)
        else:
            x_pixel_limits.append(None)
            y_pixel_limits.append(None)

    # Clean out any Nones
    x_pixel_limits = [x if x is not None else 1 for x in x_pixel_limits]
    y_pixel_limits = [y if y is not None else 1 for y in y_pixel_limits]

    return x_pixel_limits, y_pixel_limits


def get_mosaic_size(image, n_amps):
    ccdsum = image.ccdsum.split(' ')
    x_pixel_limits, y_pixel_limits = get_detsec_limits(image, n_amps)
    nx = (np.max(x_pixel_limits) - np.min(x_pixel_limits) + 1) // int(ccdsum[0])
    ny = (np.max(y_pixel_limits) - np.min(y_pixel_limits) + 1) // int(ccdsum[1])
    return nx, ny
