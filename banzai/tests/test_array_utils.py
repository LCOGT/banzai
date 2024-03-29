import numpy as np
from astropy.table import Table
import pytest

from banzai.utils import array_utils

pytestmark = pytest.mark.array_utils


def test_pruning_nans():
    a = np.arange(100, dtype=float)
    b = np.arange(100, dtype=float) + 100
    c = np.arange(100, dtype=float) + 200

    a[51] = np.nan
    b[32] = np.nan
    c[78] = np.nan

    t = Table([a, b, c], names=('a', 'b', 'c'))
    t = t[~array_utils.find_nans_in_table(t)]
    assert len(t) == 97
    assert 51 not in t['a']
    assert 32 not in t['a']
    assert 78 not in t['a']
