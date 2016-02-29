from __future__ import absolute_import, print_function, division

from astropy.io import fits
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

        for i, image in enumerate(images):
            flat_normalization = stats.mode(image.data)
            flat_data[:, :, i] = image.data / flat_normalization
            self.logger.debug('Calculating mode of {image}: mode = {mode}'.format(image=image.filename, mode=flat_normalization))
        master_flat = stats.sigma_clipped_mean(flat_data, 3.0, axis=2)

        master_flat_header = fits_utils.create_master_calibration_header(images)

        master_flat_image = Image(data=master_flat, header=master_flat_header)
        master_flat_image.filename = self.get_calibration_filename(images[0])
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

            master_flat_filename = os.path.basename(master_flat_filename)
            image.header.add_history('Master Flat: {flat_file}'.format(flat_file=master_flat_filename))

        return images
