/* Including definitions of the standard int types is necesssary for Windows,
 * and does no harm on other platforms.
 */
#include <stdint.h>

/*Get the kth element of an array "a" with length "n" using the Quickselect algorithm. */
float quick_select(float* a, int k, int n);
