#include<Python.h>
#include<stdint.h>
#include "_median_utils.h"
#include "quick_select.h"

/* Calculate the 2D median */

void _median2d(float* d, uint8_t* mask, float* output_array, int nx, int ny){
    PyDoc_STRVAR(_median2d__doc__,
    "_median2d(data, mask, output, nx, ny) -> void\n\n"
    "Calculate the median along the x-axis of a 2D array.");

    float* median_array;
    int i, j, n_unmasked_pixels, nxj, nxji;

#pragma omp parallel firstprivate(output, data, mask, nx, ny) private(i, j, nxj, nxji, median_array, n_unmasked_pixels)
    {
       /*Each thread allocates its own array. */
       median_array = (float *) malloc(nx * sizeof(float));
#pragma omp for nowait
       for (j = 0; j < ny; j++){
            n_unmasked_pixels = 0;
            nxj = nx * j;
            for (i = 0; i < nx; i++) {
                nxji = nxj + i;
                if (mask[nxji] == 0){
                    median_array[n_unmasked_pixels] = d[nxji];
                    n_unmasked_pixels++;
                    }
                }
            output_array[j] = _median1d(median_array, n_unmasked_pixels);
            }
        free(median_array);
    }
}


float _median1d(float* ptr, int n){
    PyDoc_STRVAR(_median1d__doc__,
    "_median1d(data, mask, output, nx, ny) -> void\n\n"
    "Calculate the median of a 1D array.");

    float med = 0.0;
    int k = (n - 1) / 2;
    if (n > 0){
        med = quick_select(ptr, k, n);
        if (n % 2 == 0){
            med = (med + quick_select(ptr, k + 1, n)) / 2.0;
        }
    }
    return med;
}