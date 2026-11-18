import numpy as np
import pytest
from PIL import Image

from banzai.utils import jpg_utils

pytestmark = pytest.mark.smart_stacking

STACK_SHAPE = (120, 160)
BRIGHT_STAR = (30, 40)


def make_stack(n_frames=8, seed=1):
    """A pixel sum of n noisy subs, like build_stacked_frame makes: flat sky plus a bright and a faint star."""
    rng = np.random.default_rng(seed)
    subs = rng.normal(100.0, 10.0, size=(n_frames, *STACK_SHAPE))
    subs[:, BRIGHT_STAR[0], BRIGHT_STAR[1]] += 20000.0
    subs[:, 80, 100] += 2000.0
    return subs.sum(axis=0)


def background_level(display_image):
    return float(np.median(display_image))


def test_stretch_for_display_returns_uint8_2d():
    display_image = jpg_utils.stretch_for_display(np.arange(12).reshape(3, 4))

    assert display_image.dtype == np.uint8
    assert display_image.shape == (3, 4)


def test_block_average():
    averaged = jpg_utils.block_average(np.arange(7 * 10, dtype=np.float32).reshape(7, 10), max_size=5)

    # Factor 2: the odd trailing row is dropped and each output pixel is the mean of a 2x2 block.
    assert averaged.shape == (3, 5)
    assert averaged[0, 0] == pytest.approx(np.mean([0, 1, 10, 11]))
    assert averaged[2, 4] == pytest.approx(np.mean([48, 49, 58, 59]))
    # A side shorter than the shrink factor is kept rather than averaged away.
    assert jpg_utils.block_average(np.ones((1, 100)), max_size=36).shape == (1, 100)
    # stretch_for_display shrinks its input this way.
    assert jpg_utils.stretch_for_display(np.zeros((40, 60)), max_size=36).shape == (20, 30)


@pytest.mark.parametrize('data', [np.full((5, 7), np.nan), np.full((5, 7), 3.0), np.zeros((5, 7))],
                         ids=['all-nan', 'constant', 'all-zero'])
def test_stretch_for_display_featureless_input_is_black(data):
    for progress in (0.5, 1.0):
        display_image = jpg_utils.stretch_for_display(data, progress=progress)

        np.testing.assert_array_equal(display_image, np.zeros((5, 7), dtype=np.uint8))


def test_stretch_for_display_nan_speckle_matches_median_fill():
    data = np.arange(25, dtype=float).reshape(5, 5)
    data[2, 3] = np.nan
    median_filled = data.copy()
    median_filled[2, 3] = np.nanmedian(data)

    display_image = jpg_utils.stretch_for_display(data)
    expected_image = jpg_utils.stretch_for_display(median_filled)

    np.testing.assert_array_equal(display_image, expected_image)


def test_stretch_for_display_brightens_with_progress():
    stack = make_stack()

    previews = [jpg_utils.stretch_for_display(stack, progress=progress) for progress in (0.125, 0.25, 0.5, 1.0)]

    levels = [background_level(preview) for preview in previews]
    assert all(earlier < later for earlier, later in zip(levels, levels[1:])), levels
    # The earliest preview is dark, but a bright star already shows.
    assert previews[0][STACK_SHAPE[0] - 1 - BRIGHT_STAR[0], BRIGHT_STAR[1]] > 128
    # A finished stack has its sky at the target grey level, and extra subs change nothing.
    assert levels[-1] == pytest.approx(jpg_utils.TARGET_BACKGROUND * 255, abs=1)
    np.testing.assert_array_equal(jpg_utils.stretch_for_display(stack, progress=1.5), previews[-1])


def test_stretch_for_display_is_independent_of_stack_scale():
    stack = make_stack()

    from_sum = jpg_utils.stretch_for_display(stack, progress=0.25).astype(int)
    from_mean = jpg_utils.stretch_for_display(stack / 8, progress=0.25).astype(int)

    np.testing.assert_allclose(from_sum, from_mean, atol=1)


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

    jpg_utils.save_jpg(display_image, path, max_size)

    with Image.open(path) as image:
        assert image.size == expected_size
        assert image.mode == 'L'
