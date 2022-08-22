import os.path
import logging

import numpy as np

from banzai.utils import stats
from banzai.stages import Stage
from banzai.calibrations import CalibrationStacker, CalibrationUser, CalibrationComparer
import numpy as np

logger = logging.getLogger('banzai')


class FlatSNRChecker(Stage):
    def __init__(self, runtime_context):
        super(FlatSNRChecker, self).__init__(runtime_context)

    def do_stage(self, image):
        # Make sure the median signal-to-noise ratio is over 50 for the image other abort
        flat_snr = np.median(image.data.signal_to_noise())
        logger.info('Flat signal-to-noise', image=image, extra_tags={'flat_snr': flat_snr})
        if flat_snr < 50.0:
            logger.error('Rejecting Flat due to low signal-to-noise', image=image, extra_tags={'flat_snr': flat_snr})
            return None
        else:
            return image


class FlatNormalizer(Stage):
    def __init__(self, runtime_context):
        super(FlatNormalizer, self).__init__(runtime_context)

    def do_stage(self, image):
        # Get the sigma clipped mean of the central 25% of the image
        flat_normalization = stats.sigma_clipped_mean(image.primary_hdu.get_inner_image_section(), 3.5)
        image /= flat_normalization
        image.meta['FLATLVL'] = flat_normalization
        logger.info('Calculate flat normalization', image=image,
                    extra_tags={'flat_normalization': flat_normalization})
        return image


class FlatMaker(CalibrationStacker):
    def __init__(self, runtime_context):
        super(FlatMaker, self).__init__(runtime_context)

    @property
    def calibration_type(self):
        return 'SKYFLAT'

    def make_master_calibration_frame(self, images):
        master_image = super(FlatMaker, self).make_master_calibration_frame(images)
        occulted_mask = np.zeros(master_image.shape, dtype=np.uint8)
        occulted_mask[master_image.data < 0.2] = 4
        master_image.mask |= occulted_mask
        master_image.data[master_image.mask > 0] = 1.0
        return master_image


class FlatDivider(CalibrationUser):
    def __init__(self, runtime_context):

        super(FlatDivider, self).__init__(runtime_context)

    @property
    def calibration_type(self):
        return 'SKYFLAT'

    def apply_master_calibration(self, image, master_calibration_image):

        master_flat_filename = master_calibration_image.filename
        logging_tags = {'master_flat': os.path.basename(master_calibration_image.filename)}
        logger.info('Flattening image', image=image, extra_tags=logging_tags)
        image /= master_calibration_image
        image.mask |= master_calibration_image.mask
        master_flat_filename = os.path.basename(master_flat_filename)
        image.meta['L1IDFLAT'] = (master_flat_filename, 'ID of flat frame')
        image.meta['L1STATFL'] = (1, 'Status flag for flat field correction')

        return image


class FlatComparer(CalibrationComparer):
    def __init__(self, runtime_context):
        super(FlatComparer, self).__init__(runtime_context)

    @property
    def calibration_type(self):
        return 'SKYFLAT'

    @property
    def reject_image(self):
        return False

    def noise_model(self, image):
        flat_normalization = float(image.meta['FLATLVL'])
        poisson_noise = np.where(image.data > 0, image.data * flat_normalization, 0.0)
        noise = (image.readnoise ** 2.0 + poisson_noise) ** 0.5
        noise /= flat_normalization
        return noise
