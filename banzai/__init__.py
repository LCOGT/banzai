# Licensed under a 3-clause BSD style license - see LICENSE.rst

# Packages may add whatever they like to this file, but
# should keep this content at the top.
# ----------------------------------------------------------------------------
from ._astropy_init import *  # noqa
import banzai.logs  # noqa: F401
# ----------------------------------------------------------------------------

if not _ASTROPY_SETUP_:  # noqa
    from banzai import utils
__all__ = ['utils']
