import os.path
import logging

import numpy as np

from banzai.utils import stats
from banzai.stages import Stage
from banzai.calibrations import CalibrationStacker, ApplyCalibration, CalibrationComparer

logger = logging.getLogger(__name__)


class FlatNormalizer(Stage):
    def __init__(self, pipeline_context):
        super(FlatNormalizer, self).__init__(pipeline_context)

    def do_stage(self, images):
        for image in images:
            # Get the sigma clipped mean of the central 25% of the image
            flat_normalization = stats.sigma_clipped_mean(image.get_inner_image_section(), 3.5)
            image.data /= flat_normalization
            image.header['FLATLVL'] = flat_normalization
            logger.info('Calculate flat normalization', image=image,
                             extra_tags={'flat_normalization': flat_normalization})

        return images


class FlatMaker(CalibrationStacker):
    def __init__(self, pipeline_context):
        super(FlatMaker, self).__init__(pipeline_context)

    @property
    def calibration_type(self):
        return 'skyflat'

    @property
    def group_by_attributes(self):
        return ['ccdsum', 'filter']

    @property
    def min_images(self):
        return 5

    def make_master_calibration_frame(self, images):
        master_image = super(FlatMaker, self).make_master_calibration_frame(images)
        master_image.bpm = np.logical_or(master_image.bpm, master_image.data < 0.2)
        master_image.data[master_image.bpm] = 1.0
        return master_image


class FlatDivider(ApplyCalibration):
    def __init__(self, pipeline_context):

        super(FlatDivider, self).__init__(pipeline_context)

    @property
    def master_selection_criteria(self):
        return ['ccdsum', 'filter']

    @property
    def calibration_type(self):
        return 'skyflat'

    def apply_master_calibration(self, images, master_calibration_image):

        master_flat_filename = master_calibration_image.filename
        master_flat_data = master_calibration_image.data
        logging_tags = {'master_flat': os.path.basename(master_calibration_image.filename)}
        for image in images:
            logger.info('Flattening image', image=image, extra_tags=logging_tags)
            image.data /= master_flat_data
            image.bpm |= master_calibration_image.bpm
            master_flat_filename = os.path.basename(master_flat_filename)
            image.header['L1IDFLAT'] = (master_flat_filename, 'ID of flat frame used')
            image.header['L1STATFL'] = (1, 'Status flag for flat field correction')

        return images


class FlatComparer(CalibrationComparer):
    def __init__(self, pipeline_context):
        super(FlatComparer, self).__init__(pipeline_context)

    @property
    def master_selection_criteria(self):
        return ['ccdsum', 'filter']

    @property
    def calibration_type(self):
        return 'skyflat'

    @property
    def reject_images(self):
        return False

    def noise_model(self, image):
        flat_normalization = float(image.header['FLATLVL'])
        poisson_noise = np.where(image.data > 0, image.data * flat_normalization, 0.0)
        noise = (image.readnoise ** 2.0 + poisson_noise) ** 0.5
        noise /= flat_normalization
        return noise
