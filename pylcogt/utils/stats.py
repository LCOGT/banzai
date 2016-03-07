from __future__ import absolute_import, print_function, division

import numpy as np
from scipy.stats import gamma
from astropy.modeling import custom_model

__author__ = 'cmccully'


def absolute_deviation(a, axis=None, mask=None):
    # Calculate the image deviation
    if mask is not None:
        a[mask > 0] = np.nan
    a_median = np.nanmedian(a, axis=axis)

    return np.abs(a - a_median)


def median_absolute_deviation(a, axis=None, abs_deviation=None, mask=None):
    if abs_deviation is None:
        abs_deviation = absolute_deviation(a, axis=axis, mask=mask)

    return np.nanmedian(abs_deviation, axis=axis)


def robust_standard_deviation(a, axis=None, abs_deviation=None, mask=None):
    return 1.4826 * median_absolute_deviation(a, axis=axis, abs_deviation=abs_deviation, mask=mask)


def sigma_clipped_mean(a, sigma, axis=None, mask=None):

    abs_deviation = absolute_deviation(a, axis=axis, mask=mask)

    robust_std = robust_standard_deviation(a, axis=axis, abs_deviation=abs_deviation, mask=mask)

    # Throw away any values that are N sigma from the median
    sigma_mask = abs_deviation > (sigma * robust_std)
    a[sigma_mask] = np.nan

    # Take the sigma clipped mean

    return np.nanmean(a, axis=axis)
