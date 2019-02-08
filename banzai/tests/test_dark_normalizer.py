import pytest
import numpy as np

from banzai.dark import DarkNormalizer
from banzai.tests.utils import FakeImage


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(7298374)


def test_null_input_images():
    normalizer = DarkNormalizer(None)
    image = normalizer.run(None)
    assert image is None


def test_dark_normalization_is_reasonable(set_random_seed):
    nx = 101
    ny = 103

    normalizer = DarkNormalizer(None)
    data = np.random.normal(30.0, 10, size=(ny, nx))
    image = FakeImage()
    image.data = data.copy()

    image = normalizer.do_stage(image)
    np.testing.assert_allclose(image.data, data / image.exptime, 1e-5)
