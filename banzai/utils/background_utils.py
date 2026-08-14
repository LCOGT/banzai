import numpy as np
from photutils.background import Background2D

from banzai.utils import stats


_BACKGROUND_BOX_SIZE = (32, 32)
_BACKGROUND_FILTER_SIZE = (3, 3)
_BACKGROUND_NSIGMA_CLIP = 5.0


def estimate_background(data):
    """Estimate the standard low-resolution background map."""
    # Estimate each 32x32-pixel box with Background2D's default SExtractor-style mode estimator,
    # then median-filter the low-resolution mesh with a 3x3-box window.
    return Background2D(data, _BACKGROUND_BOX_SIZE, filter_size=_BACKGROUND_FILTER_SIZE).background


def background_header_cards(background):
    """Build the L1 background-statistic FITS header cards."""
    return {
        'L1MEAN': (stats.sigma_clipped_mean(background, _BACKGROUND_NSIGMA_CLIP),
                   '[counts] Sigma clipped mean of frame background'),
        'L1MEDIAN': (np.median(background), '[counts] Median of frame background'),
        'L1SIGMA': (stats.robust_standard_deviation(background),
                    '[counts] Robust std dev of frame background'),
    }
