import logging
import numpy as np

from banzai.calibrations import CalibrationUser
from banzai.logs import format_exception

logger = logging.getLogger('banzai')


class ReadNoiseMapLoader(CalibrationUser):
    def apply_master_calibration(self, image, master_calibration_image):
        try:
            for image_extension, readnoise_extension in zip(image.ccd_hdus, master_calibration_image.ccd_hdus):
                image_extension.add_mask(readnoise_extension.data)
        except:
            logger.error(f"Can't add BPM to image, stopping reduction: {format_exception()}", image=image)
            return None
        image.meta['L1IDMASK'] = master_calibration_image.filename, 'Id. of mask file used'
        return image

    def on_missing_master_calibration(self, image):
        logger.error('Master {caltype} does not exist'.format(caltype=self.calibration_type.upper()), image=image)
        if self.runtime_context.override_missing:
            if image.data is None:
                readnoise_map = None
            else:
                readnoise_map = [np.zeros(extension.data.shape, dtype=np.uint8) for extension in image.ccd_hdus]
            for image_extension, readnoise_data in zip(image.ccd_hdus, readnoise_map):
                image_extension.add_mask(readnoise_data)
            return image
        else:
            return None

    @property
    def calibration_type(self):
        return 'NOISEMAP'