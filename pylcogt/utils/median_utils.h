/* Including definitions of the standard int types is necesssary for Windows,
 * and does no harm on other platforms.
 */
#include <stdint.h>

/*Get the kth element of an array "a" with length "n" using the Quickselect algorithm. */
float quick_select(float* a, int k, int n);

/*Get the median of an array "a" with length "n" using the Quickselect algorithm. */
float median1d(float* a, int n);

/* Get the median along the x-axis of an array "data" with dimensions
 * nx x ny. The result is stored in the output array (which should already be allocated.
 * Pixels with non-zero mask values are not included in the median */
void median2d(float* data, float* output, uint8_t* mask, int nx, int ny);