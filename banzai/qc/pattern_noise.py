import os
import numpy as np
from scipy.signal import cwt, ricker
from scipy.ndimage.filters import median_filter

from banzai.stages import Stage
from banzai import logs


class PatternNoiseDetector(Stage):
    # Signal to Noise threshold to raise an alert
    SNR_THRESHOLD = 10.0
    # The maximum allowed standard deviation of the peak centres
    STD_THRESHOLD = 1.0

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
            logs.add_tag(logging_tags, 'std_threshold', self.STD_THRESHOLD)

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
                                  'pattern_noise.snr_threshold': self.SNR_THRESHOLD,
                                  'pattern_noise.std_threshold': self.STD_THRESHOLD,
                                  }, image)
        return images

    def check_for_pattern_noise(self, data):
        """
        Test for pattern noise in an image

        Parameters
        ----------
        data : numpy array
               Image data to test for pattern noise
        band_width : float
                The fractional width of the central band to extract from the FFT
        snr_clip_fraction: float
                The fraction of pixels to clip from each SNR edge

        Returns
        -------
        is_bad : bool
                 Returns true if the image has pattern noise

        """

        trimmed_data = trim_image_edges(data)
        power_2d = get_2d_power_band(trimmed_data)
        snr = compute_snr(power_2d)
        convolved_snr = convolve_snr_with_wavelet(snr)
        peak_maxima, std_maxima = get_peak_parameters(convolved_snr)

        # Check that all peaks are above threshold and that the peak center standard deviation is small.
        has_pattern_noise = (peak_maxima > self.SNR_THRESHOLD).all() and std_maxima < self.STD_THRESHOLD
        return has_pattern_noise


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


def get_2d_power_band(data, fractional_band_width=0.25):
    """
    Extract the central region of the 2D Fourier transform

    Parameters
    ----------
    data : numpy array
        The data for computing the Fourier Transform
    fractional_band_width : float
        Vertical band width as a fraction of ny

    Returns
    -------
    power_2d : numpy array
        Central band of 2d Fourier transform
    """
    # Get full 2D power
    full_power_2d = abs(np.fft.rfft2(data))

    # Extract horizontal band, as corners of 2D FFT can vary significantly between images
    ny = full_power_2d.shape[0]
    y1 = int(ny * (0.5 - fractional_band_width/2))
    y2 = int(ny * (0.5 + fractional_band_width/2))

    return full_power_2d[y1:y2]


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
    scatter = median_filter(p2p_scatter, size=window_size)

    snr = (power - continuum) / scatter
    return snr


def get_odd_integer(x):
    """
    Return the closest odd integer given a float

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


def convolve_snr_with_wavelet(snr,
                              fractional_wavelet_width_min=0.01,
                              fractional_wavelet_width_max=0.025,
                              nwavelets=25):
    """
    Convolve the 1D SNR with nwavelets Ricker wavelets, with widths ranging from
    fractional_wavelength_width_min * len(snr) to fractional_wavelength_witdh_max * len(snr).

    Parameters
    ----------
    snr : numpy array
        The 1D SNR array
    fractional_wavelet_width_min : float
        The fraction of the SNR length to use as the minimum wavelet width
    fractional_wavelet_width_max : float
        The fraction of the SNR length to use as the maximum wavelet width
    nwavelets : int
        The number of wavelength widths to convolve with the SNR

    Returns
    -------
    snr_convolved : numpy array
        Array of size nwavelets x len(snr) of snr convolved with different wavelet widths
    """

    widths = np.linspace(len(snr) * fractional_wavelet_width_min,
                         len(snr) * fractional_wavelet_width_max,
                         nwavelets)
    return cwt(snr, ricker, widths)


def get_peak_parameters(convolved_snr):
    """

    Parameters
    ----------
    convolved_snr : numpy array
        The SNR array convolved with the Ricker wavelet of various widths

    Returns
    -------
    peak_maxima: array
        The the maximum peak value for all Ricker wavelet widths
    std_maxima: float
        The standard devation of the peak maxima across wavelet widths
    """
    peak_maxima = np.max(convolved_snr, axis=1)
    std_maxima = np.std(np.argmax(convolved_snr, axis=1))
    return peak_maxima, std_maxima
