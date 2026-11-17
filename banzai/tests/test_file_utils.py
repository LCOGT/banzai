import pytest

from banzai.utils import file_utils

pytestmark = pytest.mark.smart_stacking


@pytest.mark.parametrize(
    'smartstack_filename,expected',
    [
        (
            'tst2m002-xx06-20260213-0031-e45.fits',
            ('tst2m002-xx06-20260213-0031-e45-small_thumbnail.jpg',
             'tst2m002-xx06-20260213-0031-e45-large_thumbnail.jpg'),
        ),
        (
            'tst2m002-xx06-20260213-0031-e45.fits.fz',
            ('tst2m002-xx06-20260213-0031-e45-small_thumbnail.jpg',
             'tst2m002-xx06-20260213-0031-e45-large_thumbnail.jpg'),
        ),
    ],
)
def test_make_jpg_filenames(smartstack_filename, expected):
    filenames = file_utils.make_jpg_filenames(smartstack_filename)

    assert filenames == expected


@pytest.mark.parametrize('filename', ['tst2m002-xx06-20260213-0031-e45.txt', 'tst2m002-xx06-20260213-0031-e45'])
def test_fits_basename_rejects_unknown_extension(filename):
    with pytest.raises(ValueError, match='Could not parse FITS filename'):
        file_utils.fits_basename(filename)
