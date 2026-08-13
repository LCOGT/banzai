import numpy as np
import pytest
from PIL import Image

from banzai.utils import jpg_utils

pytestmark = pytest.mark.smart_stacking


def test_stretch_for_display_returns_uint8_2d():
    display_image = jpg_utils.stretch_for_display(np.arange(12).reshape(3, 4))

    assert display_image.dtype == np.uint8
    assert display_image.shape == (3, 4)


def test_stretch_for_display_decimates():
    display_image = jpg_utils.stretch_for_display(np.zeros((40, 60), dtype=np.float32), max_size=36)

    assert display_image.shape == (20, 30)


def test_stretch_for_display_all_nan():
    display_image = jpg_utils.stretch_for_display(np.full((5, 7), np.nan))

    np.testing.assert_array_equal(display_image, np.zeros((5, 7), dtype=np.uint8))


def test_stretch_for_display_nan_speckle_matches_median_fill():
    data = np.arange(25, dtype=float).reshape(5, 5)
    data[2, 3] = np.nan
    median_filled = data.copy()
    median_filled[2, 3] = np.nanmedian(data)

    display_image = jpg_utils.stretch_for_display(data)
    expected_image = jpg_utils.stretch_for_display(median_filled)

    np.testing.assert_array_equal(display_image, expected_image)


@pytest.mark.parametrize(
    'shape,max_size,expected_size',
    [
        pytest.param((1200, 1200), 300, (300, 300), id='small-square'),
        pytest.param((1200, 1200), 900, (900, 900), id='large-square'),
        pytest.param((600, 1200), 300, (300, 150), id='rectangular'),
    ],
)
def test_save_jpg_sizes(tmp_path, shape, max_size, expected_size):
    display_image = np.arange(np.prod(shape), dtype=np.uint32).reshape(shape).astype(np.uint8)
    path = tmp_path / f'{shape[0]}x{shape[1]}-{max_size}.jpg'

    assert jpg_utils.save_jpg(display_image, path, max_size) == path

    with Image.open(path) as image:
        assert image.size == expected_size
        assert image.mode == 'L'
