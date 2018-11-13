import logging

import numpy as np
from scipy.ndimage.filters import median_filter
from itertools import groupby
from operator import itemgetter

from banzai.stages import Stage
from banzai.utils.stats import robust_standard_deviation

logger = logging.getLogger(__name__)


class PatternNoiseDetector(Stage):

    # Signal to Noise threshold to raise an alert
    SNR_THRESHOLD = 10.0
    # The fraction of grouped SNR pixels that need to be above the threshold to raise an alert
    MIN_FRACTION_PIXELS_ABOVE_THRESHOLD = 0.01
    # The minimum number of adjacent pixels to form a group
    MIN_ADJACENT_PIXELS = 3

    def __init__(self, pipeline_context):
        super(PatternNoiseDetector, self).__init__(pipeline_context)

    def do_stage(self, images):
        for image in images:
            pattern_noise_is_bad, fraction_pixels_above_threshold = self.check_for_pattern_noise(image.data)
            logging_tags = {'snr_threshold': self.SNR_THRESHOLD,
                            'min_fraction_pixels_above_threshold': self.MIN_FRACTION_PIXELS_ABOVE_THRESHOLD,
                            'min_adjacent_pixels': self.MIN_ADJACENT_PIXELS,
                            'fraction_pixels_above_threshold': fraction_pixels_above_threshold}
            if pattern_noise_is_bad:
                logger.error('Image found to have pattern noise.', image=image, extra_tags=logging_tags)
            else:
                logger.info('No pattern noise found.', image=image, extra_tags=logging_tags)
            self.save_qc_results({'pattern_noise.failed': pattern_noise_is_bad,
                                  'pattern_noise.snr_threshold': self.SNR_THRESHOLD,
                                  'pattern_noise.min_fraction_pixels_above_threshold':
                                      self.MIN_FRACTION_PIXELS_ABOVE_THRESHOLD,
                                  'pattern_noise.min_adjacent_pixels': self.MIN_ADJACENT_PIXELS,
                                  'patter_noise.fraction_pixels_above_threshold': fraction_pixels_above_threshold
                                  }, image)
        return images

    def check_for_pattern_noise(self, data):
        """
        Test for pattern noise in an image

        Parameters
        ----------
        data : numpy array
               Image data to test for pattern noise

        Returns
        -------
        is_bad : bool
                 Returns true if the image has pattern noise

        """

        trimmed_data = trim_image_edges(data)
        power_2d = get_2d_power_band(trimmed_data)
        snr = compute_snr(power_2d)
        fraction_pixels_above_threshold = self.get_n_grouped_pixels_above_threshold(snr) / float(len(snr))

        has_pattern_noise = fraction_pixels_above_threshold > self.MIN_FRACTION_PIXELS_ABOVE_THRESHOLD
        return has_pattern_noise, fraction_pixels_above_threshold

    def get_n_grouped_pixels_above_threshold(self, snr):
        """
        Compute the number of grouped pixels above the alert threshold

        Parameters
        ----------
        snr : numpy array
            The 1D SNR
        Returns
        -------
        n_grouped_pixels_above_threshold : numpy array
            The number of SNR pixels with values above SNR_THRESHOLD
            that are in groups of at least MIN_ADJACENT_PIXELS
        """

        idx_above_thresh = np.where(snr > self.SNR_THRESHOLD)[0]
        consecutive_group_lengths = np.array([len(list(map(itemgetter(1), g))) for k, g in
                                              groupby(enumerate(idx_above_thresh), key=lambda x: x[0]-x[1])])
        pixel_groups = consecutive_group_lengths >= self.MIN_ADJACENT_PIXELS
        n_grouped_pixels_above_threshold = sum(consecutive_group_lengths[pixel_groups])
        return n_grouped_pixels_above_threshold


def trim_image_edges(data, fractional_edge_width=0.025):
    """
    Clip image edges to avoid edge effects in Fourier transform

    Parameters
    ----------
    data : numpy array
        The data to be trimmed
    fractional_edge_width : float
        Fraction of mean(nx, ny) to trim from each edge

    Returns
    -------
    trimmed_data : numpy array
        Trimmed data array
    """
    ntrim = int(round(np.mean(data.shape) * fractional_edge_width))
    return data[ntrim:-ntrim, ntrim:-ntrim]


def get_2d_power_band(data, fractional_band_width=0.25, fractional_inner_edge_to_discard=0.025):
    """
    Extract the central region of the 2D Fourier transform

    Parameters
    ----------
    data : numpy array
        The data for computing the Fourier Transform
    fractional_band_width : float
        Vertical band width as a fraction of ny
    fractional_inner_edge_to_discard : float
        Amount of inner area (i.e. where large-scale power is detected) to discard as a fraction of nx

    Returns
    -------
    power_2d : numpy array
        Central band of 2d Fourier transform
    """
    # Get full 2D power
    full_power_2d = abs(np.fft.rfft2(data))

    # Extract horizontal band, as corners of 2D FFT can vary significantly between images
    ny, nx = full_power_2d.shape
    y1 = int(ny * (0.5 - fractional_band_width/2))
    y2 = int(ny * (0.5 + fractional_band_width/2))
    x1 = int(nx * fractional_inner_edge_to_discard)

    return full_power_2d[y1:y2, x1:]


def compute_snr(power_2d, fractional_window_size=0.05):
    """
    Extract the central region of the 2D Fourier transform

    Parameters
    ----------
    power_2d : numpy array
        The 2D Fourier transform of the data
    fractional_window_size : float
        Median filter window size as a fraction of the 1D power array

    Returns
    -------
    snr : numpy array
        The 1D SNR
    """
    power = np.median(power_2d, axis=0)
    p2p_scatter = abs(power[1:] - power[:-1])
    power = power[1:]  # Throw away DC term

    # Median filter
    window_size = get_odd_integer(fractional_window_size * len(power))
    continuum = median_filter(power, size=window_size)
    pixel_to_pixel_scatter = median_filter(p2p_scatter, size=window_size)
    snr = (power - continuum) / pixel_to_pixel_scatter

    # Also divide out the global scatter for any residual structure that was not removed with the median filter
    global_scatter = robust_standard_deviation(snr)
    snr /= global_scatter

    return snr


def get_odd_integer(x):
    """
    Return the ceiling odd integer given a float

    Parameters
    ----------
    x : float
        The number to be converted to the closest odd integer

    Returns
    -------
    y : int
        Odd integer of x
    """
    return int(round(round(x) / 2) * 2) + 1
