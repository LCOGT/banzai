import os.path
import logging

import numpy as np

from banzai.utils import stats, fits_utils
from banzai.images import Image
from banzai.stages import CalibrationMaker, ApplyCalibration, CalibrationComparer, Stage

logger = logging.getLogger(__name__)


class DarkNormalizer(Stage):
    def __init__(self, pipeline_context):
        super(DarkNormalizer, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for image in images:
            image.data /= image.exptime
            logger.info('Normalizing dark by exposure time', image=image)
        return images


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

    def make_master_calibration_frame(self, images, image_config):
        dark_data = np.zeros((images[0].ny, images[0].nx, len(images)), dtype=np.float32)
        dark_mask = np.zeros((images[0].ny, images[0].nx, len(images)), dtype=np.uint8)

        master_dark_filename = self.get_calibration_filename(images[0])

        logging_tags = {'master_dark': os.path.basename(master_dark_filename)}
        for i, image in enumerate(images):
            logger.debug('Combining dark', image=image, extra_tags=logging_tags)

            dark_data[:, :, i] = image.data[:, :]
            dark_mask[:, :, i] = image.bpm[:, :]

        master_dark = stats.sigma_clipped_mean(dark_data, 3.0, axis=2, mask=dark_mask, inplace=True)

        # Memory cleanup
        del dark_data
        del dark_mask

        master_bpm = np.array(master_dark == 0.0, dtype=np.uint8)

        # Save the master dark image with all of the combined images in the header
        master_dark_header = fits_utils.create_master_calibration_header(images)
        master_dark_image = Image(self.pipeline_context, data=master_dark,
                                  header=master_dark_header)
        master_dark_image.filename = master_dark_filename
        master_dark_image.bpm = master_bpm

        logger.info('Created master dark', image=master_dark_image)
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

    def apply_master_calibration(self, images, master_calibration_image):
        master_dark_data = master_calibration_image.data
        master_dark_filename = os.path.basename(master_calibration_image.filename)
        logging_tags = {'master_dark': os.path.basename(master_calibration_image.filename)}

        for image in images:
            logger.info('Subtracting dark', image=image, extra_tags=logging_tags)
            image.data -= master_dark_data * image.exptime
            image.bpm |= master_calibration_image.bpm
            image.header['L1IDDARK'] = (master_dark_filename, 'ID of dark frame used')
            image.header['L1STATDA'] = (1, 'Status flag for dark frame correction')
        return images


class DarkComparer(CalibrationComparer):
    def __init__(self, pipeline_context):
        super(DarkComparer, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return ['ccdsum']

    @property
    def calibration_type(self):
        return 'dark'

    @property
    def reject_images(self):
        return True

    def noise_model(self, image):
        poisson_noise = np.where(image.data > 0, image.data * image.exptime, 0.0)
        noise = (image.readnoise ** 2.0 + poisson_noise) ** 0.5
        noise /= image.exptime
        return noise
