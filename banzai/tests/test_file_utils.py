import pytest

from banzai.utils import file_utils

pytestmark = pytest.mark.smart_stacking


@pytest.mark.parametrize(
    'first_filename,expected',
    [
        ('tst2m002-xx06-20260213-0031-e09.fits', 'tst2m002-xx06-20260213-0031-e45.fits'),
        ('tst2m002-xx06-20260213-0031-e09.fits.fz', 'tst2m002-xx06-20260213-0031-e45.fits.fz'),
        ('tst2m002-xx06-20260213-0031-x09.fits', 'tst2m002-xx06-20260213-0031-x45.fits'),
        ('/data/tst2m002-xx06-20260213-000031-e09.fits', 'tst2m002-xx06-20260213-000031-e45.fits'),
    ],
)
def test_make_smartstack_filename(first_filename, expected):
    assert file_utils.make_smartstack_filename(first_filename) == expected


def test_make_smartstack_filename_same_night_stacks_do_not_collide():
    first_stack = file_utils.make_smartstack_filename('tfn0m419-sq32-20260712-0202-e09.fits')
    second_stack = file_utils.make_smartstack_filename('tfn0m419-sq32-20260712-0216-e09.fits')

    assert first_stack != second_stack


def test_make_smartstack_filename_garbage_first_raises():
    with pytest.raises(ValueError):
        file_utils.make_smartstack_filename('not-a-reduced-frame.txt')


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
