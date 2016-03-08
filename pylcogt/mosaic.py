from pylcogt.stages import Stage
import numpy as np
from pylcogt.utils import fits_utils


class MosaicCreator(Stage):
    def __init__(self, pipeline_context):
        super(MosaicCreator, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for image in images:
            if len(image.data.shape) > 2:
                n_amps = image.data.shape[0]
                nx, ny = get_mosaic_size(image, n_amps)
                mosaiced_data = np.zeros((ny, nx))
                for i in range(n_amps):
                    datasec = image.header['DATASEC{0}'.format(i + 1)]
                    amp_slice = fits_utils.parse_region_keyword(datasec)
                    detsec = image.header['DETSEC{0}'.format(i + 1)]
                    mosaic_slice = fits_utils.parse_region_keyword(detsec)
                    mosaiced_data[mosaic_slice] = image.data[i, *amp_slice]

                image.data = mosaiced_data
                image.update_shape(nx, ny)
        return images


def get_mosaic_size(image, n_amps):
    x_pixel_limits = []
    y_pixel_limits = []
    for i in range(n_amps):
        header_keyword = image.header['DETSEC{0}'.format(i + 1)]
        y_slice, x_slice = fits_utils.parse_region_keyword(header_keyword)
        x_pixel_limits.append(x_slice.start)
        x_pixel_limits.append(x_slice.stop)
        y_pixel_limits.append(y_slice.start)
        y_pixel_limits.append(y_slice.stop)

    nx = np.max(x_pixel_limits) - np.min(x_pixel_limits)
    ny = np.max(y_pixel_limits) - np.min(y_pixel_limits)
    return nx, ny
