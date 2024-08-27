from banzai.calibrations import CalibrationUser
from banzai.logs import format_exception, get_logger
import numpy as np

logger = get_logger()


class ReadNoiseLoader(CalibrationUser):
    def apply_master_calibration(self, image, master_calibration_image):
        try:
            for image_extension, readnoise_extension in zip(image.ccd_hdus, master_calibration_image.ccd_hdus):
                image_extension.uncertainty = readnoise_extension.data * np.sqrt(image_extension.n_sub_exposures)
        except:
            logger.error(f"Can't add READNOISE to image, stopping reduction: {format_exception()}", image=image)
            return None
        image.meta['L1IDRDN'] = master_calibration_image.filename, 'Id. of readnoise map file used'
        return image

    def on_missing_master_calibration(self, image):
        return image

    @property
    def calibration_type(self):
        return 'READNOISE'
