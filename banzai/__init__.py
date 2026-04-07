# Licensed under a 3-clause BSD style license - see LICENSE.rst

# Packages may add whatever they like to this file, but
# should keep this content at the top.
# ----------------------------------------------------------------------------
import os
import banzai.logs  # noqa: F401
# ----------------------------------------------------------------------------

# OPENTSDB_PYTHON_METRICS_TEST_MODE activates test mode if it has been set,
# regardless of its value. Test mode is good in dev environments because it
# surpresses noisy "max retries exceeded" connection warnings, but we need it
# disabled in production to enable metrics reporting.
#
# If OPENTSDB_HOSTNAME is set, we can assume we're running in prod.
if "OPENTSDB_HOSTNAME" not in os.environ:
    os.environ["OPENTSDB_PYTHON_METRICS_TEST_MODE"] = "True"

try:
    import importlib.metadata as metadata
except ImportError:
    import importlib_metadata as metadata  # For older Python

__version__ = metadata.version("lco-banzai")

from banzai import utils
__all__ = ['utils']
