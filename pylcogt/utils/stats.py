from __future__ import absolute_import, print_function, division

import numpy as np
from scipy.stats import gamma
from astropy.modeling import custom_model

__author__ = 'cmccully'


def absolute_deviation(a, axis=None, mask=None):
    if axis is not None:
        keepdims = True
    else:
        keepdims = False
    # Calculate the image deviation
    if mask is not None:
        a[mask > 0] = np.nan
    a_median = np.nanmedian(a, axis=axis, keepdims=keepdims)

    return np.abs(a - a_median)


def median_absolute_deviation(a, axis=None, abs_deviation=None, mask=None):
    if abs_deviation is None:
        abs_deviation = absolute_deviation(a, axis=axis, mask=mask)

    if axis is not None:
        keepdims = True
    else:
        keepdims = False

    return np.nanmedian(abs_deviation, axis=axis, keepdims=keepdims)


def robust_standard_deviation(a, axis=None, abs_deviation=None, mask=None):
    return 1.4826 * median_absolute_deviation(a, axis=axis, abs_deviation=abs_deviation, mask=mask)


def sigma_clipped_mean(a, sigma, axis=None, mask=None):

    abs_deviation = absolute_deviation(a, axis=axis, mask=mask)

    robust_std = robust_standard_deviation(a, axis=axis, abs_deviation=abs_deviation, mask=mask)

    # Throw away any values that are N sigma from the median
    sigma_mask = abs_deviation > (sigma * robust_std)
    a[sigma_mask] = np.nan

    if axis is not None:
        keepdims = True
    else:
        keepdims = False
    # Take the sigma clipped mean

    return np.nanmean(a, axis=axis, keepdims=keepdims)


def mode(image_data):
    from . import fitting
    # Only used the data within +-4 sigma of the median
    data_median = np.median(image_data)
    data_std = robust_standard_deviation(image_data)
    good_data = image_data > (data_median - 4.0 * data_std)
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
    initial_model = gamma_model(normalization=hist_data.max(), a=3,
                                loc=data_median, scale=gamma_scale)
    best_fit_model = fitting.irls(x_hist, hist_data, hist_data ** 0.5, initial_model)

    return best_fit_model.loc + (best_fit_model.a - 1.0) * best_fit_model.scale
