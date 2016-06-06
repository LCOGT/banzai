#include<Python.h>
#include "quick_select.h"
#include<stdint.h>

#define ELEM_SWAP(a,b) { float t=(a); (a)=(b); (b)=t; }


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
