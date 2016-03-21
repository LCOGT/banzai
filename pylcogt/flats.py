from __future__ import absolute_import, print_function, division

import numpy as np
import os.path

from .utils import stats, fits_utils
from .stages import CalibrationMaker, ApplyCalibration
from pylcogt.images import Image

__author__ = 'cmccully'


class FlatMaker(CalibrationMaker):

    def __init__(self, pipeline_context):

        super(FlatMaker, self).__init__(pipeline_context)

    @property
    def calibration_type(self):
        return 'skyflat'

    @property
    def group_by_keywords(self):
        return ['ccdsum', 'filter']

    @property
    def min_images(self):
        return 5

    def make_master_calibration_frame(self, images, image_config, logging_tags):
        flat_data = np.zeros((images[0].ny, images[0].nx, len(images)))
        flat_mask = np.zeros((images[0].ny, images[0].nx, len(images)), dtype=np.uint8)

        quarter_nx = images[0].nx // 4
        quarter_ny = images[0].ny // 4

        for i, image in enumerate(images):

            # Get the sigma clipped mean of the central 25% of the image
            flat_normalization = stats.sigma_clipped_mean(image.data[quarter_ny: -quarter_ny,
                                                                     quarter_nx:-quarter_nx], 3.5)
            flat_data[:, :, i] = (image.data / flat_normalization)[:, :]
            flat_mask[:, :, i] = image.bpm[:, :]
            self.logger.debug('Normalization of {image} = {norm}'.format(image=image.filename,
                                                                         norm=flat_normalization))
        master_flat = stats.sigma_clipped_mean(flat_data, 3.0, axis=2, mask=flat_mask,
                                               fill_value=1.0)

        master_bpm = np.array(master_flat == 1.0, dtype=np.uint8)

        master_flat_header = fits_utils.create_master_calibration_header(images)

        master_flat_image = Image(data=master_flat, header=master_flat_header)
        master_flat_image.filename = self.get_calibration_filename(images[0])
        master_flat_image.bpm = master_bpm

        return [master_flat_image]


class FlatDivider(ApplyCalibration):
    def __init__(self, pipeline_context):

        super(FlatDivider, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return ['ccdsum', 'filter']

    @property
    def calibration_type(self):
        return 'skyflat'

    def apply_master_calibration(self, images, master_calibration_image, logging_tags):

        master_flat_filename = master_calibration_image.filename
        master_flat_data = master_calibration_image.data

        for image in images:
            self.logger.debug('Flattening {image}'.format(image=image.filename))

            image.data /= master_flat_data
            image.bpm |= master_calibration_image.bpm
            master_flat_filename = os.path.basename(master_flat_filename)
            image.header['L1IDFLAT'] = master_flat_filename

        return images
