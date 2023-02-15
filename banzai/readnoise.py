import logging

from banzai.calibrations import CalibrationUser
from banzai.logs import format_exception

logger = logging.getLogger('banzai')


class ReadNoiseLoader(CalibrationUser):
    def apply_master_calibration(self, image, master_calibration_image):
        try:
            for image_extension, readnoise_extension in zip(image.ccd_hdus, master_calibration_image.ccd_hdus):
                image_extension.add_uncertainty(readnoise_extension.data)
        except:
            logger.error(f"Can't add READNOISE to image, stopping reduction: {format_exception()}", image=image)
            return None
        image.meta['L1IDNOISE'] = master_calibration_image.filename, 'Id. of map file used'
        return image

    def on_missing_master_calibration(self, image):
        return image

    @property
    def calibration_type(self):
        return 'READNOISE'
