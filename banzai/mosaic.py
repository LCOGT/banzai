from __future__ import absolute_import, division, print_function, unicode_literals
from banzai.stages import Stage
import numpy as np
from banzai.utils import fits_utils
from banzai import logs
import os


class MosaicCreator(Stage):
    def __init__(self, pipeline_context):
        super(MosaicCreator, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for image in images:
            if len(image.data.shape) > 2:

                logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
                logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))

                n_amps = image.data.shape[0]
                nx, ny = get_mosaic_size(image, n_amps)
                mosaiced_data = np.zeros((ny, nx), dtype=np.float32)
                mosaiced_bpm = np.zeros((ny, nx), dtype=np.uint8)
                for i in range(n_amps):
                    datasec = image.header['DATASEC{0}'.format(i + 1)]
                    amp_slice = fits_utils.parse_region_keyword(datasec)
                    logs.add_tag(logging_tags, 'DATASEC{0}'.format(i + 1), datasec)

                    detsec = image.header['DETSEC{0}'.format(i + 1)]
                    mosaic_slice = fits_utils.parse_region_keyword(detsec)
                    logs.add_tag(logging_tags, 'DETSEC{0}'.format(i + 1), datasec)

                    mosaiced_data[mosaic_slice] = image.data[i][amp_slice]
                    mosaiced_bpm[mosaic_slice] = image.bpm[i][amp_slice]

                image.data = mosaiced_data
                image.bpm = mosaiced_bpm
                image.update_shape(nx, ny)
                image.header['NAXIS'] = 2
                image.header.pop('NAXIS3')

                self.logger.info('Mosaiced image', extra=logging_tags)
        return images


def get_mosaic_size(image, n_amps):
    x_pixel_limits = []
    y_pixel_limits = []
    for i in range(n_amps):
        header_keyword = image.header['DETSEC{0}'.format(i + 1)]
        y_slice, x_slice = fits_utils.parse_region_keyword(header_keyword)
        x_pixel_limits.append(x_slice.start + 1)
        x_pixel_limits.append(x_slice.stop)
        y_pixel_limits.append(y_slice.start + 1)
        y_pixel_limits.append(y_slice.stop)

    # Clean out any Nones
    x_pixel_limits = [x if x is not None else 1 for x in x_pixel_limits]
    y_pixel_limits = [y if y is not None else 1 for y in y_pixel_limits]

    nx = np.max(x_pixel_limits) - np.min(x_pixel_limits) + 1
    ny = np.max(y_pixel_limits) - np.min(y_pixel_limits) + 1
    return nx, ny
