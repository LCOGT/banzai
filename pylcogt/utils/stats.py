from __future__ import absolute_import, print_function, division
import numpy as np

from pylcogt.utils import median_utils

__author__ = 'cmccully'


def median(d, axis=None, mask=None):
    """median(d, axis=None, mask=None)\n
    Find the median of a numpy array. If an axis is provided, then find the median along
    the given axis. If mask is included, elements in d that have a non-zero mask value will
    be ignored.

    Parameters
    ----------
    d : float32 numpy array
        Input array to find the median.

    axis : int
           Index of the array to take the median
    mask: unit8 numpy array
          Numpy array of bitmask values. Non-zero values are ignored when calculating the median.

    Returns
    -------
    med : float32 numpy array
        The median value. If axis is None, then we return a single float.

    Notes
    -----
    Makes extensive use of the quick select algorithm written in C, included in median_utils.c.
    If all of the elements in the array are masked (or all of the elements of the axis of interest
    are masked), we return zero.
    """
    if axis is None:
        if mask is not None:
            median_mask = mask.ravel()
        else:
            median_mask = np.zeros(d.size, dtype=np.uint8)
        output_median = median_utils.median1d(d.ravel().astype('f4'), median_mask.astype(np.uint8))
    else:
        output_shape = np.delete(d.shape, axis)
        nx = d.shape[axis]
        ny = d.size // nx

        if mask is not None:
            median_mask = np.swapaxes(mask, axis, -1).reshape(ny, nx).astype(np.uint8)
        else:
            median_mask = np.zeros((ny, nx), dtype=np.uint8)

        med = median_utils.median2d(np.swapaxes(d, axis, -1).reshape(ny, nx).astype('f4'),
                        mask=median_mask)
        output_median = np.array(med).reshape(output_shape)

    return output_median


def absolute_deviation(a, axis=None, mask=None):
    # Calculate the image deviation
    a_median = median(a, axis=axis, mask=mask)
    if axis is not None:
        a_median = np.expand_dims(a_median, axis=axis)
    return np.abs(a - a_median)


def median_absolute_deviation(a, axis=None, abs_deviation=None, mask=None):
    if abs_deviation is None:
        abs_deviation = absolute_deviation(a, axis=axis, mask=mask)

    return median(abs_deviation, axis=axis, mask=mask)


def robust_standard_deviation(a, axis=None, abs_deviation=None, mask=None):
    return 1.4826 * median_absolute_deviation(a, axis=axis, abs_deviation=abs_deviation, mask=mask)


def sigma_clipped_mean(a, sigma, axis=None, mask=None):
    """
    """
    abs_deviation = absolute_deviation(a, axis=axis, mask=mask)

    robust_std = robust_standard_deviation(a, axis=axis, abs_deviation=abs_deviation, mask=mask)

    if axis is not None:
        robust_std = np.expand_dims(robust_std, axis=axis)

    # Throw away any values that are N sigma from the median
    sigma_mask = (abs_deviation > (sigma * robust_std))

    if mask is not None:
        sigma_mask = np.logical_or(sigma_mask, (mask > 0))

    data_copy = a.copy()
    data_copy[sigma_mask] = 0.0

    # Take the sigma clipped mean
    mean_values = data_copy.sum(axis=axis)

    n_good_pixels = (~sigma_mask).sum(axis=axis)
    if axis is None:
        if n_good_pixels > 0:
            mean_values /= n_good_pixels
    else:
        mean_values[n_good_pixels > 0] /= n_good_pixels[n_good_pixels > 0]

    return mean_values
