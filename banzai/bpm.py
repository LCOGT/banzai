import logging
import numpy as np

from banzai.calibrations import CalibrationUser
from banzai.stages import Stage
from banzai.logs import format_exception

logger = logging.getLogger('banzai')


class BadPixelMaskLoader(CalibrationUser):
    def apply_master_calibration(self, image, master_calibration_image):
        try:
            for image_extension, bpm_extension in zip(image.ccd_hdus, master_calibration_image.ccd_hdus):
                image_extension.add_mask(bpm_extension.data)
        except:
            logger.error(f"Can't add BPM to image, stopping reduction: {format_exception()}", image=image)
            return None
        image.meta['L1IDMASK'] = master_calibration_image.filename, 'Id. of mask file used'
        return image

    def on_missing_master_calibration(self, image):
        logger.error('Master {caltype} does not exist'.format(caltype=self.calibration_type.upper()), image=image)
        if self.runtime_context.no_bpm:
            if image.data is None:
                bpm = None
            else:
                bpm = [np.zeros(extension.data.shape, dtype=np.uint8) for extension in image.ccd_hdus]
            for image_extension, bpm_data in zip(image.ccd_hdus, bpm):
                image_extension.add_mask(bpm_data)
            return image
        else:
            return None

    @property
    def calibration_type(self):
        return 'BPM'


class SaturatedPixelFlagger(Stage):
    def do_stage(self, image):
        for image_extension in image.ccd_hdus:
            image_extension.mask[image_extension.data > image_extension.saturate] |= 2

        return image
