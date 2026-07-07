import pytest

from banzai.utils import file_utils

pytestmark = pytest.mark.smart_stacking


def test_make_smartstack_filename_basic():
    filename = file_utils.make_smartstack_filename('tst2m002-xx06-20260213-0031-e09.fits', 31, 42)

    assert filename == 'tst2m002-xx06-20260213-0031-0042-e45.fits'


def test_make_smartstack_filename_fz():
    filename = file_utils.make_smartstack_filename('tst2m002-xx06-20260213-0031-e09.fits.fz', 31, 42)

    assert filename == 'tst2m002-xx06-20260213-0031-0042-e45.fits.fz'


def test_make_smartstack_filename_x_kind():
    filename = file_utils.make_smartstack_filename('tst2m002-xx06-20260213-0031-x09.fits', 31, 42)

    assert filename == 'tst2m002-xx06-20260213-0031-0042-x45.fits'


def test_make_smartstack_filename_width():
    filename = file_utils.make_smartstack_filename('tst2m002-xx06-20260213-000031-e09.fits', 31, 42)

    assert filename == 'tst2m002-xx06-20260213-000031-000042-e45.fits'


def test_make_smartstack_filename_single_frame():
    filename = file_utils.make_smartstack_filename('tst2m002-xx06-20260213-0031-e09.fits', 31, 31)

    assert filename == 'tst2m002-xx06-20260213-0031-0031-e45.fits'


def test_make_smartstack_filename_garbage_raises():
    with pytest.raises(ValueError):
        file_utils.make_smartstack_filename('not-a-reduced-frame.txt', 31, 42)


@pytest.mark.parametrize(
    'smartstack_filename,expected',
    [
        (
            'tst2m002-xx06-20260213-0031-0042-e45.fits',
            ('tst2m002-xx06-20260213-0031-0042-e45-small_thumbnail.jpg',
             'tst2m002-xx06-20260213-0031-0042-e45-large_thumbnail.jpg'),
        ),
        (
            'tst2m002-xx06-20260213-0031-0042-e45.fits.fz',
            ('tst2m002-xx06-20260213-0031-0042-e45-small_thumbnail.jpg',
             'tst2m002-xx06-20260213-0031-0042-e45-large_thumbnail.jpg'),
        ),
    ],
)
def test_make_jpg_filenames(smartstack_filename, expected):
    filenames = file_utils.make_jpg_filenames(smartstack_filename)

    assert filenames == expected
    assert filenames[0].endswith('-small_thumbnail.jpg')
    assert filenames[1].endswith('-large_thumbnail.jpg')
