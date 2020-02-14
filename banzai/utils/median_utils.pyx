# cython: boundscheck=False, nonecheck=False, wraparound=False
# cython: cdivision=True
# cython: language_level=3
from __future__ import absolute_import, division, print_function, unicode_literals
from libc.stdint cimport uint8_t
from libc.stdlib cimport malloc, free
import numpy as np
cimport numpy as np

cimport cython
from cython.parallel import parallel, prange

np.import_array()

cdef extern from "quick_select.h":
    float quick_select(float * k, int k, int n) nogil


@cython.boundscheck(False)
@cython.wraparound(False)
def _quick_select(float[::1] a not None, int k):
    cdef float value
    cdef int size = a.size
    with nogil:
        value = quick_select(&a[0], k, size)
    return value


@cython.boundscheck(False)
@cython.wraparound(False)
cdef float _cmedian1d(float* ptr, int n) nogil:
    cdef float med = 0.0
    cdef int k = (n - 1) // 2
    if n > 0:
        med = quick_select(ptr, k, n)
        if n % 2 == 0:
            med += quick_select(ptr, k + 1, n)
            med /= 2.0
    return med


@cython.boundscheck(False)
@cython.wraparound(False)
def median1d(float[::1] d not None, uint8_t[::1] mask not None):
    """_median1d(d, mask)\n
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
    are masked), we return zero. This has comparable performance to np.median on unmasked data,
    but does not require the gil. For masked arrays, the performance is significantly better
    (anecdotally, I have seen improvements of more than order of magnitude, but I have not done
    a comprehensive benchmark).
    """

    cdef int n = d.shape[0]

    cdef float[::1] median_array = np.empty(n, dtype=np.float32)

    cdef int n_unmasked_pixels = 0
    cdef int i = 0
    for i in range(n):
        if mask[i] == 0:
            median_array[n_unmasked_pixels] = d[i]
            n_unmasked_pixels += 1

    return _cmedian1d(&median_array[0], n_unmasked_pixels)


@cython.boundscheck(False)
@cython.wraparound(False)
def median2d(float[:, ::1] d, uint8_t[:, ::1] mask):

    cdef int nx = d.shape[1]
    cdef int ny = d.shape[0]

    cdef int j = 0
    cdef int i

    cdef float[::1] output_array = np.empty(ny, dtype=np.float32)
    cdef float* median_array
    cdef int n_unmasked_pixels = 0

    with nogil, parallel():
        median_array = <float *> malloc(nx * sizeof(float))
        for j in prange(ny):
            n_unmasked_pixels = 0
            for i in range(nx):
                if mask[j, i] == 0:
                    median_array[n_unmasked_pixels] = d[j, i]
                    n_unmasked_pixels = n_unmasked_pixels + 1

            output_array[j] = _cmedian1d(median_array, n_unmasked_pixels)
        free(median_array)
    return output_array
