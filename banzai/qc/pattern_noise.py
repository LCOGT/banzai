import os
import numpy as np
from scipy.signal import cwt, ricker

from banzai.stages import Stage
from banzai import logs


class PatternNoiseDetector(Stage):
    # Signal to Noise threshold to raise an alert
    SNR_THRESHOLD = 5.0
    # Fraction of convolution widths that need to be above S/N threshold to raise an alert
    FRAC_WIDTHS_THRESHOLD = 0.9
    # The maximum allowed standard deviation of the peak centres
    STD_THRESHOLD = 1.0
    # Minimum convolution wavelet minimum
    WAVELET_WIDTH_MIN = 25
    # Maximum convolution wavelet minimum
    WAVELET_WIDTH_MAX = 50

    def __init__(self, pipeline_context):
        super(PatternNoiseDetector, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for image in images:
            logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            logs.add_tag(logging_tags, 'snr_threshold', self.SNR_THRESHOLD)
            logs.add_tag(logging_tags, 'frac_widths_threshold', self.FRAC_WIDTHS_THRESHOLD)
            logs.add_tag(logging_tags, 'std_threshold', self.STD_THRESHOLD)
            logs.add_tag(logging_tags, 'wavelet_width_min', self.WAVELET_WIDTH_MIN)
            logs.add_tag(logging_tags, 'wavelet_width_max', self.WAVELET_WIDTH_MAX)

            # If the data is a cube, then run on each extension individually
            if image.data_is_3d():
                pattern_noise_is_bad = any([self.check_for_pattern_noise(data) for data in image.data])
            else:
                pattern_noise_is_bad = self.check_for_pattern_noise(image.data)

            if pattern_noise_is_bad:
                self.logger.error('Image found to have pattern noise.', extra=logging_tags)
            else:
                self.logger.info('No pattern noise found.', extra=logging_tags)
            self.save_qc_results({'pattern_noise.failed': pattern_noise_is_bad,
                                  'pattern_noise.wavelet_width_min': self.WAVELET_WIDTH_MIN,
                                  'pattern_noise.wavelet_width_max': self.WAVELET_WIDTH_MAX,
                                  'pattern_noise.snr_threshold': self.SNR_THRESHOLD,
                                  'pattern_noise.frac_widths_threshold': self.FRAC_WIDTHS_THRESHOLD,
                                  'pattern_noise.std_threshold': self.STD_THRESHOLD,
                                  }, image)
        return images

    def check_for_pattern_noise(self, data, image_clip=100, band_width=0.25, snr_clip_fraction=0.05):
        """
        Test for pattern noise in an image

        Parameters
        ----------
        data : numpy array
               Image data to test for pattern noise
        image_clip : int
                Number of pixels to clip from each edge
        band_width : float
                The fractional width of the central band to extract from the FFT
        snr_clip_fraction: float
                The fraction of pixels to clip from each SNR edge

        Returns
        -------
        is_bad : bool
                 Returns true if the image has pattern noise

        """

        # Throw away image edges
        data = data[image_clip:-image_clip, image_clip:-image_clip]

        # Get full 2D power
        full_power_2d = np.abs(np.fft.rfft2(data))

        # Extract horizontal band, as corners of 2D FFT can vary significantly between images
        ny = full_power_2d.shape[0]
        y1 = round(ny * (0.5 - band_width/2))
        y2 = round(ny * (0.5 + band_width/2))
        power_2d = full_power_2d[y1:y2]

        # Get 1D SNR
        power = np.median(power_2d, axis=0)[1:]  # Throw away DC term
        p2p_scatter = (np.sum((power[1:]-power[:-1])**2) / (len(power)-2))**0.5
        snr = (power - np.median(power)) / p2p_scatter

        # Convolve with wavelet to find peaks, throwing away edges
        widths = np.arange(self.WAVELET_WIDTH_MIN, self.WAVELET_WIDTH_MAX)
        snr_convolved = cwt(snr, ricker, widths)
        n_snr_clip = round(snr_clip_fraction * len(snr))
        snr_convolved = snr_convolved[:, n_snr_clip:-n_snr_clip]

        # Calculate fraction of peaks above threshold and the standard deviation of their centres
        peak_maxima = np.max(snr_convolved, axis=1)
        frac_maxima_above_snr_thresh = sum(peak_maxima > self.SNR_THRESHOLD) / len(peak_maxima)
        peak_indices = np.argmax(snr_convolved, axis=1)
        std_maxima = np.std(peak_indices)

        # Check that enough peaks are above threshold and that the peak center standard deviation is small.
        # Also, the result cannot be trusted if any of the peaks are at the edge of the SNR array.
        has_pattern_noise = (frac_maxima_above_snr_thresh > self.FRAC_WIDTHS_THRESHOLD
                             and std_maxima < self.STD_THRESHOLD
                             and not any(peak_indices == 0)
                             and not any(peak_indices == len(snr_convolved)-1))
        return has_pattern_noise
