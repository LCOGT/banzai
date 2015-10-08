import numpy as np
from scipy.stats import gamma
from astropy.modeling import custom_model

__author__ = 'cmccully'


def absolute_deviation(a, axis=None):
    # Calculate the image deviation
    a_median = np.median(a, axis=axis)

    if axis is not None:
        a_median = np.expand_dims(a_median, axis=axis)

    return np.abs(a - a_median)


def median_absolute_deviation(a, axis=None, abs_deviation=None):
    if abs_deviation is None:
        abs_deviation = absolute_deviation(a, axis=axis)
    return np.median(abs_deviation, axis=axis)


def robust_standard_deviation(a, axis=None, abs_deviation=None):
    return 1.4826 * median_absolute_deviation(a, axis=axis, abs_deviation=abs_deviation)


def sigma_clipped_mean(a, sigma, axis=None):

    abs_deviation = absolute_deviation(a, axis=axis)

    robust_std = robust_standard_deviation(a, axis=axis, abs_deviation=abs_deviation)
    if axis is not None:
        robust_std = np.expand_dims(robust_std, axis=axis)

    # Throw away any values that are N sigma from the median
    mask = abs_deviation > (sigma * robust_std)
    a[mask] = 0.0

    # Take the sigma clipped mean
    mean_value = a.sum(axis=axis)
    mean_value /= np.logical_not(mask).sum(axis=axis)
    return mean_value


def mode(image_data):
    from . import fitting
    # Only used the data within +-4 sigma of the median
    data_median = np.median(image_data)
    data_std = robust_standard_deviation(image_data)
    good_data = image_data > (data_median- 4.0 * data_std)
    good_data &= image_data < (data_median + 4.0 * data_std)
    clipped_data = image_data[good_data]

    # Make a 1000 bin histogram of the data
    hist = np.histogram(clipped_data.ravel(), bins=1000, normed=False)
    hist_data = hist[0]
    x_hist = 0.5 * (hist[1][:-1] + hist[1][1:])

    # Fit the histogram with a Gamma distribution
    # The Gamma distribution doesn't require a symmetric distribution
    def gamma_pdf(x, normalization=1.0, a=1.0, loc=0, scale=1.0):
        return normalization * gamma.pdf(x, a, loc, scale)

    gamma_model = custom_model(gamma_pdf)
    gamma_scale = data_std / np.sqrt(3.0)
    initial_model = gamma_model(nomalization=hist_data.max(), a=3,
                                loc=data_median, scale=gamma_scale)
    best_fit_model = fitting.irls(x_hist, hist_data, hist_data ** 0.5, initial_model)

    return best_fit_model.loc + (best_fit_model.a - 1.0) * best_fit_model.scale
