from __future__ import absolute_import, division, print_function, unicode_literals

import numpy as np
import os.path

from banzai.utils import stats, fits_utils
from banzai.stages import CalibrationMaker, ApplyCalibration
from banzai.images import Image
from banzai import logs

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
        flat_data = np.zeros((images[0].ny, images[0].nx, len(images)), dtype=np.float32)
        flat_mask = np.zeros((images[0].ny, images[0].nx, len(images)), dtype=np.uint8)

        quarter_nx = images[0].nx // 4
        quarter_ny = images[0].ny // 4

        master_flat_filename = self.get_calibration_filename(images[0])
        logs.add_tag(logging_tags, 'master_flat', os.path.basename(master_flat_filename))
        for i, image in enumerate(images):

            # Get the sigma clipped mean of the central 25% of the image
            flat_normalization = stats.sigma_clipped_mean(image.data[quarter_ny: -quarter_ny,
                                                                     quarter_nx:-quarter_nx], 3.5)
            flat_data[:, :, i] = image.data[:, :]
            flat_data[:, :, i] /= flat_normalization
            flat_mask[:, :, i] = image.bpm[:, :]
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            logs.add_tag(logging_tags, 'flat_normalization', flat_normalization)
            self.logger.debug('Calculating flat normalization', extra=logging_tags)

        logs.pop_tag(logging_tags, 'flat_normalization')
        master_flat = stats.sigma_clipped_mean(flat_data, 3.0, axis=2, mask=flat_mask,
                                               fill_value=1.0, inplace=True)

        master_bpm = np.array(master_flat == 1.0, dtype=np.uint8)

        master_bpm = np.logical_and(master_bpm, master_flat < 0.2)

        master_flat[master_flat < 0.2] = 1.0

        master_flat_header = fits_utils.create_master_calibration_header(images)

        master_flat_image = Image(data=master_flat, header=master_flat_header)
        master_flat_image.filename = master_flat_filename
        master_flat_image.bpm = master_bpm

        logs.pop_tag(logging_tags, 'master_flat')
        logs.add_tag(logging_tags, 'filename', os.path.basename(master_flat_image.filename))
        self.logger.info('Created master flat', extra=logging_tags)

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
        logs.add_tag(logging_tags, 'master_flat',
                     os.path.basename(master_calibration_image.filename))
        for image in images:
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            self.logger.info('Flattening image', extra=logging_tags)
            image.data /= master_flat_data
            image.bpm |= master_calibration_image.bpm
            master_flat_filename = os.path.basename(master_flat_filename)
            image.header['L1IDFLAT'] = (master_flat_filename, 'ID of flat frame used')
            image.header['L1STATFL'] = (1, 'Status flag for flat field correction')

        return images
