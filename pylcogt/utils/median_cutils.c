#include<Python.h>
#include "median_cutils.h"
#include "quick_select.h"
#include<stdint.h>

float _cmedian1d(float* a, int n){
    float med = 0.0;
    int k = (n - 1) / 2;
    if (n > 0) {
        med = quick_select(a, k, n);
        if (n % 2 == 0){
            med = med + quick_select(a, k + 1, n);
            med = med / 2.0;
        }
    }
    return med;
}

void _cmedian2d(float* d, uint8_t* mask, float* output, int nx, int ny){
    float* median_array;
    int n_unmasked_pixels;
    int i, j, nxj;

#pragma omp parallel firstprivate(data, output, mask, nx, ny) private(i, j, nxj, n_unmasked_pixels, median_array)
    {
        /*Each thread allocates its own array. */
        median_array = (float *) malloc(nx * sizeof(float));

#pragma omp for nowait
        for (j=0; j < ny; j++){
            n_unmasked_pixels = 0;
            nxj = nx * j;
            for(i=0; i < nx; i++){
                if (mask[nxj + i] == 0){
                    median_array[n_unmasked_pixels] = d[nxj + i];
                    n_unmasked_pixels = n_unmasked_pixels + 1;
                }
            }
        output[j] = _cmedian1d(median_array, n_unmasked_pixels);
        }
        free(median_array);
    }
}
