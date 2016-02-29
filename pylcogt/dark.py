from __future__ import absolute_import, print_function, division

from astropy.io import fits
import numpy as np
import os.path

from .utils import stats, fits_utils
from pylcogt.images import Image
from .stages import CalibrationMaker, ApplyCalibration

__author__ = 'cmccully'


class DarkMaker(CalibrationMaker):
    def __init__(self, pipeline_context):

        super(DarkMaker, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return ['ccdsum']

    @property
    def calibration_type(self):
        return 'DARK'

    @property
    def min_images(self):
        return 5

    def make_master_calibration_frame(self, images, image_config, logging_tags):
        dark_data = np.zeros((images[0].ny, images[0].nx, len(images)))

        for i, image in enumerate(images):
            self.logger.debug('Combining dark {filename}'.format(filename=image.filename))

            dark_data[:, :, i] = image.data / image.exptime

        master_dark = stats.sigma_clipped_mean(dark_data, 3.0, axis=2)

        # Save the master dark image with all of the combined images in the header
        master_dark_header = fits_utils.create_master_calibration_header(images)
        master_dark_image = Image(data=master_dark, header=master_dark_header)
        master_dark_image.filename = self.get_calibration_filename(images[0])

        return [master_dark_image]


class DarkSubtractor(ApplyCalibration):
    def __init__(self, pipeline_context):
        super(DarkSubtractor, self).__init__(pipeline_context)

    @property
    def calibration_type(self):
        return 'dark'

    @property
    def group_by_keywords(self):
        return ['ccdsum']

    def apply_master_calibration(self, images, master_calibration_image, logging_tags):
        master_dark_data = master_calibration_image.data
        master_dark_filename = os.path.basename(master_calibration_image.filename)
        for image in images:
            self.logger.debug('Subtracting dark for {image}'.format(image=image.filename))
            image.data -= master_dark_data * image.exptime
            image.header.add_history('Master Dark: {dark_file}'.format(dark_file=master_dark_filename))

        return images
