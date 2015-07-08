__author__ = 'cmccully'

from astropy.stats import median_absolute_deviation

def robust_standard_deviation(a, axis=None):
    return 1.4826 * median_absolute_deviation(a, axis=axis)
