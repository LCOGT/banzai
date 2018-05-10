import os
import numpy as np

from banzai.stages import Stage
from banzai import logs
from banzai.utils.stats import median_absolute_deviation


class PatternNoiseDetector(Stage):
    # Signal to Noise threshold to raise an alert
    SNR_THRESHOLD = 15.0
    # Number of pixels that need to be above the S/N threshold to raise an alert
    FREQ_BINS_THRESHOLD = 5

    def __init__(self, pipeline_context):
        super(PatternNoiseDetector, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for image in images:
            # If the data is a cube, then run on each extension individually
            logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            logs.add_tag(logging_tags, 'snr_threshold', self.SNR_THRESHOLD)
            logs.add_tag(logging_tags, 'freq_bins_threshold', self.FREQ_BINS_THRESHOLD)
            data_is_3d = len(image.data.shape) > 2
            if data_is_3d:
                pattern_noise_is_bad = any([check_for_pattern_noise(data, self.SNR_THRESHOLD, self.FREQ_BINS_THRESHOLD)
                                            for data in image.data])
            else:
                pattern_noise_is_bad = check_for_pattern_noise(image.data, self.SNR_THRESHOLD, self.FREQ_BINS_THRESHOLD)
            if pattern_noise_is_bad:
                self.logger.error('Image found to have pattern noise.', extra=logging_tags)
            else:
                self.logger.info('No pattern noise found.', extra=logging_tags)
            self.save_qc_results({'pattern_noise.failed': pattern_noise_is_bad,
                                  'pattern_noise.snr_threshold': self.SNR_THRESHOLD,
                                  'pattern_noise.freq_bins_threshold': self.FREQ_BINS_THRESHOLD}, image)
        return images


def check_for_pattern_noise(data, snr_threshold, freq_bins_threshold):
    """
    Test for pattern noise in an image

    Parameters
    ----------
    data : numpy array
           Image data to test for pattern noise
    snr_threshold : float
                    Threshold for the Signal to Noise ratio in the power spectrum to be considered bad
    freq_bins_threshold : int
                      Number of frequencey bins that have to be above the S/N threshold for an image to
                      be considered bad

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
    return (snr > snr_threshold).sum() >= freq_bins_threshold
