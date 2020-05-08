# Licensed under a 3-clause BSD style license - see LICENSE.rst

# Packages may add whatever they like to this file, but
# should keep this content at the top.
# ----------------------------------------------------------------------------
from ._astropy_init import *
from banzai.logs import BanzaiLogger
from lcogt_logging import LCOGTFormatter
import logging

# ----------------------------------------------------------------------------

# Uncomment to enforce Python version check during package import.
# This is the same check as the one at the top of setup.py
import sys
#class UnsupportedPythonError(Exception):
#    pass
#__minimum_python_version__ = '3.5'
#if sys.version_info < tuple((int(val) for val in __minimum_python_version__.split('.'))):
#    raise UnsupportedPythonError("{} does not support Python < {}".format(__package__, __minimum_python_version__))

if not _ASTROPY_SETUP_:
    from banzai import utils
__all__ = ['utils']

logging.setLoggerClass(BanzaiLogger)
logging.captureWarnings(True)
logger = logging.getLogger('banzai')

handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(LCOGTFormatter())
logger.addHandler(handler)

# Default the logger to INFO so that we actually get messages by default.
logger.setLevel('INFO')
