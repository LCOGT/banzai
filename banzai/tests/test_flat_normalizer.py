import pytest
import numpy as np

from banzai.flats import FlatNormalizer
from banzai.tests.utils import FakeImage


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(9723492)


def test_null_input_image():
    normalizer = FlatNormalizer(None)
    image = normalizer.run(None)
    assert image is None


def test_header_has_flatlevel():
    normalizer = FlatNormalizer(None)
    image = normalizer.do_stage(FakeImage(image_multiplier=2.0))
    assert image.header['FLATLVL'] == 2.0


def test_header_flatlevel_is_5():
    normalizer = FlatNormalizer(None)
    image = normalizer.do_stage(FakeImage(image_multiplier=5.0))
    assert image.header['FLATLVL'] == 5.0


def test_flat_normalization_is_reasonable(set_random_seed):
    flat_variation = 0.05
    input_level = 10000.0
    nx = 101
    ny = 103

    normalizer = FlatNormalizer(None)
    image = FakeImage()
    flat_pattern = np.random.normal(1.0, flat_variation, size=(ny, nx))
    image.data = np.random.poisson(flat_pattern * input_level).astype(float)
    image = normalizer.do_stage(image)

    # For right now, we only use a quarter of the image to calculate the flat normalization
    # because real ccds have crazy stuff at the edges, so the S/N is cut down by a factor of 2
    # Assume 50% slop because the variation in the pattern does not decrease like sqrt(n)
    assert np.abs(image.header['FLATLVL'] - input_level) < (3.0 * flat_variation * input_level / (nx * ny) ** 0.5)
    assert np.abs(np.mean(image.data) - 1.0) <= 3.0 * flat_variation / (nx * ny) ** 0.5
