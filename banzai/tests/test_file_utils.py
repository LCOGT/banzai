import pytest

from banzai.utils import file_utils

pytestmark = pytest.mark.smart_stacking


@pytest.mark.parametrize('filename', ['tst2m002-xx06-20260213-0031-e45.fits',
                                      'tst2m002-xx06-20260213-0031-e45.fits.fz'])
def test_fits_basename_strips_extension(filename):
    assert file_utils.fits_basename(filename) == 'tst2m002-xx06-20260213-0031-e45'


@pytest.mark.parametrize('filename', ['tst2m002-xx06-20260213-0031-e45.txt', 'tst2m002-xx06-20260213-0031-e45'])
def test_fits_basename_rejects_unknown_extension(filename):
    with pytest.raises(ValueError, match='Could not parse FITS filename'):
        file_utils.fits_basename(filename)
