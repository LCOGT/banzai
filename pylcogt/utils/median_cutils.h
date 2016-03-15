/* Including definitions of the standard int types is necesssary for Windows,
 * and does no harm on other platforms.
 */
#include <stdint.h>

/* Calculate the median given a 1-d array of length n*/
float _cmedian1d(float* a, int n);

/* Calculate the median of a 2-d array with given mask of size nx x ny. Output is stored in given
   output array */
void _cmedian2d(float* d, uint8_t* mask, float* output, int nx, int ny);
