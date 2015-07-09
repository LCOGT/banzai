__author__ = 'cmccully'

import numpy as np

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
