/*
 * Author: Curtis McCully
 * October 2014
 * Licensed under a 3-clause BSD style license - see LICENSE.rst
 *
 * Originally written in C++ in 2011
 * See also https://github.com/cmccully/lacosmicx
 *
 * This file contains utility functions for Lacosmicx. These are the most
 * computationally expensive pieces of the calculation so they have been ported
 * to C.
 *
 * Many thanks to Nicolas Devillard who wrote the optimized methods for finding
 * the median and placed them in the public domain. I have noted in the
 * comments places that use Nicolas Devillard's code.
 *
 * Parallelization has been achieved using OpenMP. Using a compiler that does
 * not support OpenMP, e.g. clang currently, the code should still compile and
 * run serially without issue. I have tried to be explicit as possible about
 * specifying which variables are private and which should be shared, although
 * we never actually have any shared variables. We use firstprivate instead.
 * This does mean that it is important that we never have two threads write to
 * the same memory position at the same time.
 *
 * All calculations are done with 32 bit floats to keep the memory footprint
 * small.
 */
#include<stdlib.h>
#include<stdio.h>
#include<math.h>
#include<Python.h>
#include "imutils.h"
#define ELEM_SWAP(a,b) { float t=(a);(a)=(b);(b)=t; }

float
PyMedian(float* arr, int n)
{
    /* Get the median of an array "a" with length "n"
     * using the Quickselect algorithm. Returns a float.
     * This Quickselect routine is based on the algorithm described in
     * "Numerical recipes in C", Second Edition, Cambridge University Press,
     * 1992, Section 8.5, ISBN 0-521-43108-5
     * This code by Nicolas Devillard - 1998. Public domain.
     */

    PyDoc_STRVAR(PyMedian__doc__, "PyMedian(a, n) -> float\n\n"
        "Get the median of array a of length n using the Quickselect "
        "algorithm.");

    /* Make a copy of the array so that we don't alter the input array */
    // float* arr = (float *) malloc(n * sizeof(float));
    /* Indices of median, low, and high values we are considering */
    int low = 0;
    int high = n - 1;
    int median = (low + high) / 2;
    /* Running indices for the quick select algorithm */
    int middle, ll, hh;
    /* The median to return */
    float med;

    /* running index i */
    int i;
    /* Copy the input data into the array we work with */
   // for (i = 0; i < n; i++) {
    //    arr[i] = a[i];
   // }

    /* Short circuit the median if the array has a simple length */
    if (n == 3) {
        return PyOptMed3(arr);
    }
    else if (n == 5) {
        return PyOptMed5(arr);
    }
    else if (n == 7) {
        return PyOptMed7(arr);
    }
    else if (n == 9) {
        return PyOptMed9(arr);
    }
    else if (n == 25) {
        return PyOptMed25(arr);
    }
    /* Start an infinite loop */
    while (true) {

        /* Only One or two elements left */
        if (high <= low + 1) {
            /* Check if we need to swap the two elements */
            if ((high == low + 1) && (arr[low] > arr[high]))
                ELEM_SWAP(arr[low], arr[high]);
            med = arr[median];
            free(arr);
            return med;
        }

        /* Find median of low, middle and high items;
         * swap into position low */
        middle = (low + high) / 2;
        if (arr[middle] > arr[high])
            ELEM_SWAP(arr[middle], arr[high]);
        if (arr[low] > arr[high])
            ELEM_SWAP(arr[low], arr[high]);
        if (arr[middle] > arr[low])
            ELEM_SWAP(arr[middle], arr[low]);

        /* Swap low item (now in position middle) into position (low+1) */
        ELEM_SWAP(arr[middle], arr[low + 1]);

        /* Nibble from each end towards middle,
         * swap items when stuck */
        ll = low + 1;
        hh = high;
        while (true) {
            do
                ll++;
            while (arr[low] > arr[ll]);
            do
                hh--;
            while (arr[hh] > arr[low]);

            if (hh < ll)
                break;

            ELEM_SWAP(arr[ll], arr[hh]);
        }

        /* Swap middle item (in position low) back into
         * the correct position */
        ELEM_SWAP(arr[low], arr[hh]);

        /* Re-set active partition */
        if (hh <= median)
            low = ll;
        if (hh >= median)
            high = hh - 1;
    }

}

#undef ELEM_SWAP

/* All of the optimized median methods below were written by
 * Nicolas Devillard and are in the public domain.
 */

#define PIX_SORT(a,b) { if (a>b) PIX_SWAP(a,b); }
#define PIX_SWAP(a,b) { float temp=a; a=b; b=temp; }

/* ----------------------------------------------------------------------------
 Function :   PyOptMed3()
 In       :   pointer to array of 3 pixel values
 Out      :   a pixel value
 Job      :   optimized search of the median of 3 pixel values
 Notice   :   found on sci.image.processing
 cannot go faster unless assumptions are made on the nature of the input
 signal.
 Code adapted from Nicolas Devillard.
 --------------------------------------------------------------------------- */
float
PyOptMed3(float* p)
{
    PyDoc_STRVAR(PyOptMed3__doc__, "PyOptMed3(a) -> float\n\n"
        "Get the median of array a of length 3 using a search tree.");

    PIX_SORT(p[0], p[1]);
    PIX_SORT(p[1], p[2]);
    PIX_SORT(p[0], p[1]);
    return p[1];
}

/* ----------------------------------------------------------------------------
 Function :   PyOptMed5()
 In       :   pointer to array of 5 pixel values
 Out      :   a pixel value
 Job      :   optimized search of the median of 5 pixel values
 Notice   :   found on sci.image.processing
 cannot go faster unless assumptions are made on the nature of the input
 signal.
 Code adapted from Nicolas Devillard.
 --------------------------------------------------------------------------- */
float
PyOptMed5(float* p)
{
    PyDoc_STRVAR(PyOptMed5__doc__, "PyOptMed5(a) -> float\n\n"
        "Get the median of array a of length 5 using a search tree.");

    PIX_SORT(p[0], p[1]);
    PIX_SORT(p[3], p[4]);
    PIX_SORT(p[0], p[3]);
    PIX_SORT(p[1], p[4]);
    PIX_SORT(p[1], p[2]);
    PIX_SORT(p[2], p[3]);
    PIX_SORT(p[1], p[2]);
    return p[2];
}

/* ----------------------------------------------------------------------------
 Function :   PyOptMed7()
 In       :   pointer to array of 7 pixel values
 Out      :   a pixel value
 Job      :   optimized search of the median of 7 pixel values
 Notice   :   found on sci.image.processing
 cannot go faster unless assumptions are made on the nature of the input
 signal.
 Code adapted from Nicolas Devillard.
 --------------------------------------------------------------------------- */
float
PyOptMed7(float* p)
{
    PyDoc_STRVAR(PyOptMed7__doc__, "PyOptMed7(a) -> float\n\n"
        "Get the median of array a of length 7 using a search tree.");

    PIX_SORT(p[0], p[5]);
    PIX_SORT(p[0], p[3]);
    PIX_SORT(p[1], p[6]);
    PIX_SORT(p[2], p[4]);
    PIX_SORT(p[0], p[1]);
    PIX_SORT(p[3], p[5]);
    PIX_SORT(p[2], p[6]);
    PIX_SORT(p[2], p[3]);
    PIX_SORT(p[3], p[6]);
    PIX_SORT(p[4], p[5]);
    PIX_SORT(p[1], p[4]);
    PIX_SORT(p[1], p[3]);
    PIX_SORT(p[3], p[4]);
    return p[3];
}

/* ----------------------------------------------------------------------------
 Function :   PyOptMed9()
 In       :   pointer to an array of 9 pixel values
 Out      :   a pixel value
 Job      :   optimized search of the median of 9 pixel values
 Notice   :   in theory, cannot go faster without assumptions on the
 signal.
 Formula from:
 XILINX XCELL magazine, vol. 23 by John L. Smith

 The input array is modified in the process
 The result array is guaranteed to contain the median
 value in middle position, but other elements are NOT sorted.
 Code adapted from Nicolas Devillard.
 --------------------------------------------------------------------------- */
float
PyOptMed9(float* p)
{
    PyDoc_STRVAR(PyOptMed9__doc__, "PyOptMed9(a) -> float\n\n"
        "Get the median of array a of length 9 using a search tree.");

    PIX_SORT(p[1], p[2]);
    PIX_SORT(p[4], p[5]);
    PIX_SORT(p[7], p[8]);
    PIX_SORT(p[0], p[1]);
    PIX_SORT(p[3], p[4]);
    PIX_SORT(p[6], p[7]);
    PIX_SORT(p[1], p[2]);
    PIX_SORT(p[4], p[5]);
    PIX_SORT(p[7], p[8]);
    PIX_SORT(p[0], p[3]);
    PIX_SORT(p[5], p[8]);
    PIX_SORT(p[4], p[7]);
    PIX_SORT(p[3], p[6]);
    PIX_SORT(p[1], p[4]);
    PIX_SORT(p[2], p[5]);
    PIX_SORT(p[4], p[7]);
    PIX_SORT(p[4], p[2]);
    PIX_SORT(p[6], p[4]);
    PIX_SORT(p[4], p[2]);
    return p[4];
}

/* ----------------------------------------------------------------------------
 Function :   PyOptMed25()
 In       :   pointer to an array of 25 pixel values
 Out      :   a pixel value
 Job      :   optimized search of the median of 25 pixel values
 Notice   :   in theory, cannot go faster without assumptions on the
 signal.
 Code taken from Graphic Gems.
 Code adapted from Nicolas Devillard.
 --------------------------------------------------------------------------- */
float
PyOptMed25(float* p)
{
    PyDoc_STRVAR(PyOptMed25__doc__, "PyOptMed25(a) -> float\n\n"
        "Get the median of array a of length 25 using a search tree.");

    PIX_SORT(p[0], p[1]);
    PIX_SORT(p[3], p[4]);
    PIX_SORT(p[2], p[4]);
    PIX_SORT(p[2], p[3]);
    PIX_SORT(p[6], p[7]);
    PIX_SORT(p[5], p[7]);
    PIX_SORT(p[5], p[6]);
    PIX_SORT(p[9], p[10]);
    PIX_SORT(p[8], p[10]);
    PIX_SORT(p[8], p[9]);
    PIX_SORT(p[12], p[13]);
    PIX_SORT(p[11], p[13]);
    PIX_SORT(p[11], p[12]);
    PIX_SORT(p[15], p[16]);
    PIX_SORT(p[14], p[16]);
    PIX_SORT(p[14], p[15]);
    PIX_SORT(p[18], p[19]);
    PIX_SORT(p[17], p[19]);
    PIX_SORT(p[17], p[18]);
    PIX_SORT(p[21], p[22]);
    PIX_SORT(p[20], p[22]);
    PIX_SORT(p[20], p[21]);
    PIX_SORT(p[23], p[24]);
    PIX_SORT(p[2], p[5]);
    PIX_SORT(p[3], p[6]);
    PIX_SORT(p[0], p[6]);
    PIX_SORT(p[0], p[3]);
    PIX_SORT(p[4], p[7]);
    PIX_SORT(p[1], p[7]);
    PIX_SORT(p[1], p[4]);
    PIX_SORT(p[11], p[14]);
    PIX_SORT(p[8], p[14]);
    PIX_SORT(p[8], p[11]);
    PIX_SORT(p[12], p[15]);
    PIX_SORT(p[9], p[15]);
    PIX_SORT(p[9], p[12]);
    PIX_SORT(p[13], p[16]);
    PIX_SORT(p[10], p[16]);
    PIX_SORT(p[10], p[13]);
    PIX_SORT(p[20], p[23]);
    PIX_SORT(p[17], p[23]);
    PIX_SORT(p[17], p[20]);
    PIX_SORT(p[21], p[24]);
    PIX_SORT(p[18], p[24]);
    PIX_SORT(p[18], p[21]);
    PIX_SORT(p[19], p[22]);
    PIX_SORT(p[8], p[17]);
    PIX_SORT(p[9], p[18]);
    PIX_SORT(p[0], p[18]);
    PIX_SORT(p[0], p[9]);
    PIX_SORT(p[10], p[19]);
    PIX_SORT(p[1], p[19]);
    PIX_SORT(p[1], p[10]);
    PIX_SORT(p[11], p[20]);
    PIX_SORT(p[2], p[20]);
    PIX_SORT(p[2], p[11]);
    PIX_SORT(p[12], p[21]);
    PIX_SORT(p[3], p[21]);
    PIX_SORT(p[3], p[12]);
    PIX_SORT(p[13], p[22]);
    PIX_SORT(p[4], p[22]);
    PIX_SORT(p[4], p[13]);
    PIX_SORT(p[14], p[23]);
    PIX_SORT(p[5], p[23]);
    PIX_SORT(p[5], p[14]);
    PIX_SORT(p[15], p[24]);
    PIX_SORT(p[6], p[24]);
    PIX_SORT(p[6], p[15]);
    PIX_SORT(p[7], p[16]);
    PIX_SORT(p[7], p[19]);
    PIX_SORT(p[13], p[21]);
    PIX_SORT(p[15], p[23]);
    PIX_SORT(p[7], p[13]);
    PIX_SORT(p[7], p[15]);
    PIX_SORT(p[1], p[9]);
    PIX_SORT(p[3], p[11]);
    PIX_SORT(p[5], p[17]);
    PIX_SORT(p[11], p[17]);
    PIX_SORT(p[9], p[17]);
    PIX_SORT(p[4], p[10]);
    PIX_SORT(p[6], p[12]);
    PIX_SORT(p[7], p[14]);
    PIX_SORT(p[4], p[6]);
    PIX_SORT(p[4], p[7]);
    PIX_SORT(p[12], p[14]);
    PIX_SORT(p[10], p[14]);
    PIX_SORT(p[6], p[7]);
    PIX_SORT(p[10], p[12]);
    PIX_SORT(p[6], p[10]);
    PIX_SORT(p[6], p[17]);
    PIX_SORT(p[12], p[17]);
    PIX_SORT(p[7], p[17]);
    PIX_SORT(p[7], p[10]);
    PIX_SORT(p[12], p[18]);
    PIX_SORT(p[7], p[12]);
    PIX_SORT(p[10], p[18]);
    PIX_SORT(p[12], p[20]);
    PIX_SORT(p[10], p[20]);
    PIX_SORT(p[10], p[12]);

    return p[12];
}

#undef PIX_SORT
#undef PIX_SWAP

/* We have slightly unusual boundary conditions for all of the median filters
 * below. Rather than padding the data, we just don't calculate the median
 * filter for pixels around the border of the output image (n - 1) / 2 from
 * the edge, where we are using an n x n median filter. Edge effects often
 * look like cosmic rays and the edges are often blank so this shouldn't
 * matter. We fill the border with the original data values.
 */

/* Calculate the 3x3 median filter of an array data that has dimensions
 * nx x ny. The results are saved in the output array. The output array should
 * already be allocated as we work on it in place. The median filter is not
 * calculated for a 1 pixel border around the image. These pixel values are
 * copied from the input data. The data should be striped along the x
 * direction, such that pixel i,j in the 2D image should have memory location
 * data[i + nx *j].
 */
void
PyMedCombine(float* data, float* output, int nx, int ny, int nimages)
{
    PyDoc_STRVAR(PyMedCombine__doc__,
        "PyMedCombine(data, output, nx, ny, nimages) -> void\n\n"
            "Median combine a set of images represented as a 3D array with size"
            "nx x ny x nimages. The results are saved in the output array. "
            "The output array should already be allocated as we work on it in "
            "place. The data array needs to be striped in the x direction such "
            "that pixel j,k for image i has memory location "
            "data[i + nimages * j + nimages * nx * k]. Output should be nx x ny.");

    /* Loop indices */
    int i, j, k, nxj, nximk, nxk;

    /* The array to calculate the median. Note that
     * these both need to be unique for each thread so they both need to be
     * private and we wait to allocate memory until the pragma below.*/
    float* medarr;

    /* Each thread needs to access the data and the output so we make them
     * firstprivate. We make sure that our algorithm doesn't have multiple
     * threads read or write the same piece of memory. */
#pragma omp parallel firstprivate(output, data, nx, ny, nimages) \
    private(i, j, k, medarr, nxj, nxk, nximk)
    {
        /*Each thread allocates its own array. */
        medarr = (float *) malloc(nimages * sizeof(float));

        /* Go through each pixel. Each pixel is independent so this is
         * embarrassingly parallel. */
#pragma omp for nowait
        for (k = 0; k < ny; k++) {
            /* Precalculate the multiplication nx * j, minor optimization */
            nximk = nimages * nx * k;
            nxk = nx * k;
            for (j = 0; j < nx; j++) {
                nxj = j * nimages;

                for (i = 0; i < nimages; i++) {
                    medarr[i] = data[i + nxj + nximk];
                }
                /* Calculate the median in the fastest way possible */
                output[nxk + j] = PyMedian(medarr, nimages);
            }
        }
        /* Each thread needs to free its own copy of medarr */
        free(medarr);
    }

    return;
}
