#include<Python.h>
#include "median_utils.h"
#include<stdint.h>

#define ELEM_SWAP(a,b) { float t=(a);(a)=(b);(b)=t; }


float
quick_select(float* a, int k, int n)
{
    /* Get the kth element of an array "a" with length "n"
     * using the Quickselect algorithm. Returns a float.
     * This Quickselect routine is based on the algorithm described in
     * "Numerical recipes in C", Second Edition, Cambridge University Press,
     * 1992, Section 8.5, ISBN 0-521-43108-5
     * This code adapted from code by Nicolas Devillard - 1998. Used with permission. Public domain.
     */

    PyDoc_STRVAR(quick_select__doc__, "quick_select(a, k, n) -> float\n\n"
        "Get the kth element of an array, a, of length n using the Quickselect "
        "algorithm.");

    /* Indices of low, and high values we are considering */
    int low = 0;
    int high = n - 1;
    /* Running indices for the quick select algorithm */
    int middle, ll, hh;
    /* The value to return */
    float value;

    /* running index i */
    int i;

    /* Start an infinite loop */
    while (1) {

        /* Only One or two elements left */
        if (high <= low + 1) {
            /* Check if we need to swap the two elements */
            if ((high == low + 1) && (a[low] > a[high]))
                ELEM_SWAP(a[low], a[high]);
            value = a[k];
            return value;
        }

        /* Find median of low, middle and high items;
         * swap into position low */
        middle = (low + high) / 2;
        if (a[middle] > a[high])
            ELEM_SWAP(a[middle], a[high]);
        if (a[low] > a[high])
            ELEM_SWAP(a[low], a[high]);
        if (a[middle] > a[low])
            ELEM_SWAP(a[middle], a[low]);

        /* Swap low item (now in position middle) into position (low+1) */
        ELEM_SWAP(a[middle], a[low + 1]);

        /* Nibble from each end towards middle,
         * swap items when stuck */
        ll = low + 1;
        hh = high;
        while (1) {
            do
                ll++;
            while (a[low] > a[ll]);
            do
                hh--;
            while (a[hh] > a[low]);

            if (hh < ll)
                break;

            ELEM_SWAP(a[ll], a[hh]);
        }

        /* Swap middle item (in position low) back into
         * the correct position */
        ELEM_SWAP(a[low], a[hh]);

        /* Re-set active partition */
        if (hh <= k)
            low = ll;
        if (hh >= k)
            high = hh - 1;
    }

}


#undef ELEM_SWAP


float median1d(float* a, int n) {
    PyDoc_STRVAR(quick_select__doc__, "median1d(a, n) -> float\n\n"
        "Get the median of an array, a, of length n using the Quickselect "
        "algorithm.");
    float med = quick_select(a, (n - 1) / 2, n);
    /* I suspect that you don't have to rerun the whole quickselect routine for an even number of
       elements, but that the central two indices are correctly sorted already.
       In principle this could lead to a speedup, but I haven't worked that out yet. */
    if (n % 2 == 0){
        med = med + quick_select(a, (n - 1) / 2 + 1, n);
        med = med / 2.0;
    }
    return med;
}


void median2d(float* data, float* output, uint8_t* mask, int nx, int ny)
{
    PyDoc_STRVAR(median2d__doc__,
        "median2d(data, output, mask, nx, ny) -> void\n\n"
            "Calculate the median along the x-axis for an array data with dimensions "
            "nx x ny. The results are saved in the output array. The output "
            "array should already be allocated as we work on it in place. Note "
            "that the data array needs to be striped in the x direction such "
            "that pixel x,y has memory location data[x + nx * y]");

    /* Loop indices */
    int i, j, nxj, nxji;

    /* Array to calculate the median and a counter index. Note that
     * these both need to be unique for each thread so they both need to be
     * private and we wait to allocate memory until the pragma below. */
    float* medarr;
    int medcounter;

    /* Each thread needs to access the data and the output so we make them
     * firstprivate. We make sure that our algorithm doesn't have multiple
     * threads read or write the same piece of memory. */
#pragma omp parallel firstprivate(output, data, mask, nx, ny) private(i, j, medarr, nxj, nxji, medcounter)
    {
        /*Each thread allocates its own array. */
        medarr = (float *) malloc(nx * sizeof(float));

        /* Go through each pixel*/
#pragma omp for nowait
        for (j = 0; j < ny; j++) {
            /* Precalculate the multiplication nx * j, minor optimization */
            nxj = nx * j;
            medcounter = 0;
            for (i = 0; i < nx; i++){
                nxji = nxj + i;
                if (mask[nxji] == 0) {
                    medarr[medcounter] = data[nxji];
                    medcounter++;
                    }
                }
            if (medcounter == 0){
                    output[j] = 0.0;
            }
            else{
                 /* Calculate the median */
                 output[j] = median1d(medarr, medcounter);
            }
        }

        /* Each thread needs to free its own copy of medarr */
        free(medarr);
    }

    return;
}