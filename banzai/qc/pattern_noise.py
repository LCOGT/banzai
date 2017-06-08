from banzai.stages import Stage
from banzai.utils.stats import median_absolute_deviation
from banzai.qc.utils import save_qc_results
import numpy as np

class PatternNoiseDetector(Stage):
    # Signal to Noise threshold to raise an alert
    snr_threshold = 15.0
    # Number of pixels that need to be above the S/N threshold to raise an alert
    pixel_threshold = 5

    def __init__(self, pipeline_context):
        super(PatternNoiseDetector, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for image in images:
            # If the data is a cube, then run on each extension individually
            if len(image.data.shape) > 2:
                for data in image.data:
                    pattern_noise_is_bad = check_for_pattern_noise(data, self.snr_threshold, self.pixel_threshold)
            else:
                pattern_noise_is_bad = check_for_pattern_noise(image.data, self.snr_threshold, self.pixel_threshold)

            if pattern_noise_is_bad:
                save_qc_results({'PatternNoise': True}, image)
        return images


def check_for_pattern_noise(data, snr_threshold, pixel_threshold):
    """
    Test for pattern noise in an image

    Parameters
    ----------
    data : numpy array
           Image data to test for pattern noise
    snr_threshold : float
                    Threshold for the Signal to Noise ratio in the power spectrum to be considered bad
    pixel_threshold : int
                      Number of pixels that have to be above the S/N threshold for an image to be considered bad

    Returns
    -------
    is_bad : bool
             Returns true if the image has pattern noise

    """
    power = np.median(np.abs(np.fft.rfft2(data)), axis=0)
    snr = (power - np.median(power)) / median_absolute_deviation(power)
    # Throw away the first several elements of the snr because they are usually high
    # It is not clear exactly how many you should throw away, 15 seems to work
    snr = snr[15:]
    return (snr > snr_threshold).sum() >= pixel_threshold
