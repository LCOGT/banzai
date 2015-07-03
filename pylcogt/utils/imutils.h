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
float
PyMedian(float* arr, int n);

/* All of the optimized median methods below were written by
 * Nicolas Devillard and are in the public domain.
 */

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
PyOptMed3(float* p);

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
PyOptMed5(float* p);

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
PyOptMed7(float* p);

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
PyOptMed9(float* p);

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
PyOptMed25(float* p);

void
PyMedCombine(float* data, float* output, int nx, int ny, int nimages);
