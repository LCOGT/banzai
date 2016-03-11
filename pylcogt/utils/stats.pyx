# cython: boundscheck=False, nonecheck=False, wraparound=False
# cython: cdivision=True
from __future__ import absolute_import, print_function, division

import numpy as np

from libc.stdint cimport uint8_t

cimport numpy as np

np.import_array()


__author__ = 'cmccully'

cdef extern from "median_utils.h":
    float median1d(float * a, int n) nogil
    void median2d(float * data, float * output, uint8_t * mask, int nx, int ny) nogil

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
            median_mask = None
        med = _median1d(d.ravel(), median_mask)
    else:
        output_shape = np.delete(d.shape, axis)
        nx = d.shape[axis]
        ny = d.size // nx

        if mask is not None:
            median_mask = np.swapaxes(mask, axis, -1).reshape(ny, nx)
        else:
            median_mask = None

        med = _median2d(np.swapaxes(d, axis, -1).reshape(ny, nx),
                        mask=median_mask)
        med = med.reshape(output_shape)

    return med


def _median1d(np.ndarray d, np.ndarray mask=None):
    """median(d, mask=None)\n
    Find the median of a numpy array. If an axis is provided, then find the median along
    the given axis. If mask is included, elements in d that have a non-zero mask value will
    be ignored.

    Parameters
    ----------
    d : float numpy array
        Input array to find the median.

    mask: unit8 numpy array
          Numpy array of bitmask values. Non-zero values are ignored when calculating the median.

    Returns
    -------
    med : float
        The median value.

    Notes
    -----
    Makes extensive use of the quick select algorithm written in C, included in median_utils.c.
    If all of the elements in the array are masked (or all of the elements of the axis of interest
    are masked), we return zero.
    """

    cdef int n = d.size

    cdef np.ndarray mask_array = np.zeros(n, dtype=np.uint8)
    cdef uint8_t [:] mask_memview = mask_array

    if mask is not None:
        mask_array[:] = mask.ravel()[:]

    cdef np.ndarray median_array = np.zeros(n, dtype=np.float32)
    cdef float [:] median_array_memoryview = median_array

    cdef int n_unmasked_pixels = 0
    cdef int i = 0
    for i in range(n):
        if mask_memview[i] == 0:
            median_array_memoryview[n_unmasked_pixels] = d[i]
            n_unmasked_pixels += 1

    cdef float * median_array_pointer = < float * > np.PyArray_DATA(median_array)
    cdef float med = 0.0

    if n_unmasked_pixels > 0:
        with nogil:
           med = median1d(median_array_pointer, n_unmasked_pixels)

    return med


def _median2d(np.ndarray d, np.ndarray mask=None):

    cdef int nx = d.shape[1]
    cdef int ny = d.shape[0]

    # Copy the array into memory. This likely isn't necessary, but it forces the data to be
    # contiguous in memory
    cdef np.ndarray median_array = np.zeros((ny, nx), dtype=np.float32)
    cdef float [:, :] median_memview = median_array

    cdef int i = 0
    cdef int j = 0
    for j in range(ny):
        for i in range(nx):
            median_memview[j, i] = d[j, i]

    cdef np.ndarray mask_array = np.zeros((ny, nx), dtype=np.uint8)
    cdef uint8_t [:, :] mask_memview = mask_array

    if mask is not None:
        for j in range(ny):
            for i in range(nx):
                mask_memview[j, i] = mask[j, i]

    cdef float * median_array_pointer = < float * > np.PyArray_DATA(median_array)

    cdef np.ndarray output_array = np.zeros(ny, dtype=np.float32)
    cdef float * output_array_pointer = < float * > np.PyArray_DATA(output_array)

    cdef uint8_t * mask_pointer = < uint8_t * > np.PyArray_DATA(mask_array)

    with nogil:
        median2d(median_array_pointer, output_array_pointer, mask_pointer, nx, ny)

    return output_array


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
        sigma_mask |= (mask > 0)
    n_good_pixels = sigma_mask.size - sigma_mask.sum(axis=axis)
    data_copy = a.copy()
    data_copy[sigma_mask] = 0.0

    # Take the sigma clipped mean
    mean_values = data_copy.sum(axis=axis)
    if axis is None:
        if n_good_pixels > 0:
            mean_values /= n_good_pixels
    else:
        mean_values[n_good_pixels > 0] /= n_good_pixels[n_good_pixels > 0]

    return mean_values
