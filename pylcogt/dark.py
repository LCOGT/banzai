from __future__ import absolute_import, print_function, division

import numpy as np
import os.path

from .utils import stats, fits_utils
from pylcogt import logs
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
        dark_data = np.zeros((images[0].ny, images[0].nx, len(images)), dtype=np.float32)
        dark_mask = np.zeros((images[0].ny, images[0].nx, len(images)), dtype=np.uint8)

        master_dark_filename = self.get_calibration_filename(images[0])

        logs.add_tag(logging_tags, 'master_dark', os.path.basename(master_dark_filename))
        for i, image in enumerate(images):
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            self.logger.debug('Combining dark', extra=logging_tags)

            dark_data[:, :, i] = image.data[:, :]
            dark_data[:, :, i] /= image.exptime
            dark_mask[:, :, i] = image.bpm[:, :]

        master_dark = stats.sigma_clipped_mean(dark_data, 3.0, axis=2, mask=dark_mask, inplace=True)

        # Memory cleanup
        del dark_data
        del dark_mask

        master_bpm = np.array(master_dark == 0.0, dtype=np.uint8)
        master_dark[master_bpm] = 0.0

        # Save the master dark image with all of the combined images in the header
        master_dark_header = fits_utils.create_master_calibration_header(images)
        master_dark_image = Image(data=master_dark, header=master_dark_header)
        master_dark_image.filename = master_dark_filename
        master_dark_image.bpm = master_bpm

        logs.pop_tag(logging_tags, 'master_dark')
        logs.add_tag(logging_tags, 'filename', os.path.basename(master_dark_image.filename))
        self.logger.info('Created master dark', extra=logging_tags)
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
        logs.add_tag(logging_tags, 'master_dark',
                     os.path.basename(master_calibration_image.filename))

        for image in images:
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            self.logger.info('Subtracting dark', extra=logging_tags)
            image.data -= master_dark_data * image.exptime
            image.bpm |= master_calibration_image.bpm
            image.header['L1IDDARK'] = (master_dark_filename, 'ID of dark frame used')
            image.header['L1STATDA'] = (1, 'Status flag for dark frame correction')
        return images
