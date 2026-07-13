import pytest

from banzai.utils import file_utils

pytestmark = pytest.mark.smart_stacking


def test_make_smartstack_filename_basic():
    filename = file_utils.make_smartstack_filename('tst2m002-xx06-20260213-0031-e09.fits',
                                                   'tst2m002-xx06-20260213-0042-e09.fits')

    assert filename == 'tst2m002-xx06-20260213-0031-0042-e45.fits'


def test_make_smartstack_filename_fz():
    filename = file_utils.make_smartstack_filename('tst2m002-xx06-20260213-0031-e09.fits.fz',
                                                   'tst2m002-xx06-20260213-0042-e09.fits.fz')

    assert filename == 'tst2m002-xx06-20260213-0031-0042-e45.fits.fz'


def test_make_smartstack_filename_x_kind():
    filename = file_utils.make_smartstack_filename('tst2m002-xx06-20260213-0031-x09.fits',
                                                   'tst2m002-xx06-20260213-0042-x09.fits')

    assert filename == 'tst2m002-xx06-20260213-0031-0042-x45.fits'


def test_make_smartstack_filename_width():
    filename = file_utils.make_smartstack_filename('tst2m002-xx06-20260213-000031-e09.fits',
                                                   'tst2m002-xx06-20260213-000042-e09.fits')

    assert filename == 'tst2m002-xx06-20260213-000031-000042-e45.fits'


def test_make_smartstack_filename_single_frame():
    filename = file_utils.make_smartstack_filename('tst2m002-xx06-20260213-0031-e09.fits',
                                                   'tst2m002-xx06-20260213-0031-e09.fits')

    assert filename == 'tst2m002-xx06-20260213-0031-0031-e45.fits'


def test_make_smartstack_filename_nonsequential_file_numbers():
    # File numbers, not stack positions, drive the range: a group whose inputs are
    # frames 0216..0227 must not be named 0001-0012.
    filename = file_utils.make_smartstack_filename('tfn0m419-sq32-20260712-0216-e09.fits',
                                                   'tfn0m419-sq32-20260712-0227-e09.fits')

    assert filename == 'tfn0m419-sq32-20260712-0216-0227-e45.fits'


def test_make_smartstack_filename_same_night_stacks_do_not_collide():
    # Regression: two stacks from one camera on one night must get distinct product
    # names even when they contain the same number of frames.
    first_stack = file_utils.make_smartstack_filename('tfn0m419-sq32-20260712-0202-e09.fits',
                                                      'tfn0m419-sq32-20260712-0207-e09.fits')
    second_stack = file_utils.make_smartstack_filename('tfn0m419-sq32-20260712-0216-e09.fits',
                                                       'tfn0m419-sq32-20260712-0221-e09.fits')

    assert first_stack != second_stack


def test_make_smartstack_filename_garbage_first_raises():
    with pytest.raises(ValueError):
        file_utils.make_smartstack_filename('not-a-reduced-frame.txt',
                                            'tst2m002-xx06-20260213-0042-e09.fits')


def test_make_smartstack_filename_garbage_last_raises():
    with pytest.raises(ValueError):
        file_utils.make_smartstack_filename('tst2m002-xx06-20260213-0031-e09.fits',
                                            'not-a-reduced-frame.txt')


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
