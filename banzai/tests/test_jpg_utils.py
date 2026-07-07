import numpy as np
import pytest
from PIL import Image

from banzai.utils import jpg_utils

pytestmark = pytest.mark.smart_stacking


def test_stretch_for_display_returns_uint8_2d():
    display_image = jpg_utils.stretch_for_display(np.arange(12).reshape(3, 4))

    assert display_image.dtype == np.uint8
    assert display_image.shape == (3, 4)
    assert display_image.ndim == 2


def test_stretch_for_display_decimates():
    display_image = jpg_utils.stretch_for_display(np.zeros((4000, 6000), dtype=np.float32), max_size=3600)

    assert display_image.shape == (2000, 3000)


def test_stretch_for_display_all_nan():
    display_image = jpg_utils.stretch_for_display(np.full((5, 7), np.nan))

    assert np.isfinite(display_image).all()


def test_stretch_for_display_nan_speckle_matches_median_fill():
    data = np.arange(25, dtype=float).reshape(5, 5)
    data[2, 3] = np.nan
    median_filled = data.copy()
    median_filled[2, 3] = np.nanmedian(data)

    display_image = jpg_utils.stretch_for_display(data)
    expected_image = jpg_utils.stretch_for_display(median_filled)

    np.testing.assert_array_equal(display_image, expected_image)


def test_save_jpg_sizes(tmp_path):
    display_image = np.arange(1200 * 1200, dtype=np.uint32).reshape(1200, 1200).astype(np.uint8)
    rectangular_display_image = np.zeros((600, 1200), dtype=np.uint8)
    small_path = tmp_path / 'small.jpg'
    large_path = tmp_path / 'large.jpg'
    rectangular_path = tmp_path / 'rectangular.jpg'

    assert jpg_utils.save_jpg(display_image, small_path, 300) == small_path
    assert jpg_utils.save_jpg(display_image, large_path, 900) == large_path
    jpg_utils.save_jpg(rectangular_display_image, rectangular_path, 300)

    with Image.open(small_path) as image:
        assert image.size == (300, 300)
        assert image.mode == 'L'
    with Image.open(large_path) as image:
        assert image.size == (900, 900)
        assert image.mode == 'L'
    with Image.open(rectangular_path) as image:
        assert image.size == (300, 150)
        assert image.mode == 'L'
