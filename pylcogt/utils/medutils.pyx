# cython: profile=True, boundscheck=False, nonecheck=False, wraparound=False
# cython: cdivision=True
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import numpy as np
cimport numpy as np
cimport cython

from cython cimport floating
np.import_array()

from libcpp cimport bool

from cython.parallel cimport parallel, prange

from libc.stdint cimport uint8_t
from libc.stdlib cimport abort, malloc, free

cdef extern from "imutils.h":
    float PyMedian(float * a, int n) nogil
    float PyOptMed3(float * a) nogil
    float PyOptMed5(float * a) nogil
    float PyOptMed7(float * a) nogil
    float PyOptMed9(float * a) nogil
    float PyOptMed25(float * a) nogil
    void PyMedCombine(float * data, float * output, int nx, int ny, int nimages) nogil

def median(np.ndarray[np.float32_t, mode='c', cast=True] a, int n):
    """median(a, n)\n
    Find the median of the first n elements of an array.

    Parameters
    ----------
    a : float numpy array
        Input array to find the median.

    n : int
        Number of elements of the array to median.

    Returns
    -------
    med : float
        The median value.

    Notes
    -----
    Wrapper for PyMedian in laxutils.
    """
    cdef float * aptr = < float * > np.PyArray_DATA(a)
    cdef float med = 0.0
    with nogil:
        if n == 3:
            med = PyOptMed3(aptr)
        elif n == 5:
            med = PyOptMed5(aptr)
        elif n == 7:
            med = PyOptMed7(aptr)
        elif n == 9:
            med = PyOptMed9(aptr)
        elif n == 25:
            med = PyOptMed25(aptr)
        else:
            med = PyMedian(aptr, n)
    return med


def optmed3(np.ndarray[np.float32_t, ndim=1, mode='c', cast=True] a):
    """optmed3(a)\n
    Optimized method to find the median value of an array of length 3.

    Parameters
    ----------
    a : float numpy array
        Input array to find the median. Must be length 3.

    Returns
    -------
    med3 : float
        The median of the 3-element array.

    Notes
    -----
    Wrapper for PyOptMed3 in laxutils.
    """
    cdef float * aptr3 = < float * > np.PyArray_DATA(a)
    cdef float med3 = 0.0
    with nogil:
        med3 = PyOptMed3(aptr3)
    return med3


def optmed5(np.ndarray[np.float32_t, ndim=1, mode='c', cast=True] a):
    """optmed5(a)\n
    Optimized method to find the median value of an array of length 5.

    Parameters
    ----------
    a : float numpy array
        Input array to find the median. Must be length 5.

    Returns
    -------
    med5 : float
        The median of the 5-element array.

    Notes
    -----
    Wrapper for PyOptMed5 in laxutils.
    """
    cdef float * aptr5 = < float * > np.PyArray_DATA(a)
    cdef float med5 = 0.0
    with nogil:
        med5 = PyOptMed5(aptr5)
    return med5


def optmed7(np.ndarray[np.float32_t, ndim=1, mode='c', cast=True] a):
    """optmed7(a)\n
    Optimized method to find the median value of an array of length 7.

    Parameters
    ----------
    a : float numpy array
        Input array to find the median. Must be length 7.

    Returns
    -------
    med7 : float
        The median of the 7-element array.

    Notes
    -----
    Wrapper for PyOptMed7 in laxutils.
    """
    cdef float * aptr7 = < float * > np.PyArray_DATA(a)
    cdef float med7 = 0.0
    with nogil:
        med7 = PyOptMed7(aptr7)
    return med7


def optmed9(np.ndarray[np.float32_t, ndim=1, mode='c', cast=True] a):
    """optmed9(a)\n
    Optimized method to find the median value of an array of length 9.

    Parameters
    ----------
    a : float numpy array
        Input array to find the median. Must be length 9.

    Returns
    -------
    med9 : float
        The median of the 9-element array.

    Notes
    -----
    Wrapper for PyOptMed9 in laxutils.
    """
    cdef float * aptr9 = < float * > np.PyArray_DATA(a)
    cdef float med9 = 0.0
    with nogil:
        med9 = PyOptMed9(aptr9)
    return med9


def optmed25(np.ndarray[np.float32_t, ndim=1, mode='c', cast=True] a):
    """optmed25(a)\n
    Optimized method to find the median value of an array of length 25.

    Parameters
    ----------
    a : float numpy array
        Input array to find the median. Must be length 25.

    Returns
    -------
    med25 : float
        The median of the 25-element array.

    Notes
    -----
    Wrapper for PyOptMed25 in laxutils.
    """
    cdef float * aptr25 = < float * > np.PyArray_DATA(a)
    cdef float med25 = 0.0
    with nogil:
        med25 = PyOptMed25(aptr25)
    return med25


def medcombine(np.ndarray[np.float32_t, ndim=3, mode='c', cast=True] d):
    """medcombine(d)\n
    Median combine an array of images.

    Parameters
    ----------
    d : float numpy array
        3D array of images

    Returns
    -------
    output : float numpy array
        Median filtered array.

    Notes
    -----
    Wrapper for PyMedCombine in imutils.h.
    """
    cdef int nimages = d.shape[2]
    cdef int nx = d.shape[1]
    cdef int ny = d.shape[0]

    # Allocate the output array here so that Python tracks the memory and will
    # free the memory when we are finished with the output array.
    output = np.zeros((ny, nx), dtype=np.float32)
    cdef float * dptr = < float * > np.PyArray_DATA(d)
    cdef float * outptr = < float * > np.PyArray_DATA(output)
    with nogil:
        PyMedCombine(dptr, outptr, nx, ny, nimages)

    return output
