# Licensed under a 3-clause BSD style license - see LICENSE.rst

# Packages may add whatever they like to this file, but
# should keep this content at the top.
# ----------------------------------------------------------------------------
import os
import banzai.logs  # noqa: F401
# ----------------------------------------------------------------------------

# Remove useless warning: max retries exceeded on host scheduler-dev.lco.gtn
os.environ.setdefault("OPENTSDB_PYTHON_METRICS_TEST_MODE", "1")

try:
    import importlib.metadata as metadata
except ImportError:
    import importlib_metadata as metadata  # For older Python

__version__ = metadata.version("lco-banzai")

from banzai import utils
__all__ = ['utils']
