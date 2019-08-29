import logging

from banzai.calibrations import CalibrationUser
from banzai.stages import Stage

logger = logging.getLogger('banzai')


class BadPixelMaskLoader(CalibrationUser):
    def apply_master_calibration(self, image, master_calibration_image):
        for image_extension, bpm_extension in zip(image.image_extensions, master_calibration_image.image_extensions):
            image_extension.add_mask(bpm_extension.data)
        image.meta['L1IDMASK'] = master_calibration_image.filename, 'Id. of mask file used'
        return image


class SaturatedPixelFlagger(Stage):
    def do_stage(self, image):
        for image_extension in image.image_extensions:
            image_extension.mask[image_extension.data > image_extension.saturate] |= 2
