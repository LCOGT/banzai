import numpy as np
import pytest

from banzai.dark import DarkNormalizer
from banzai.tests.utils import FakeImage


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(7298374)


def test_null_input_image(set_random_seed):
    normalizer = DarkNormalizer(None)
    image = normalizer.run(None)
    assert image is None


def test_dark_normalization_is_reasonable(set_random_seed):
    nx = 101
    ny = 103

    normalizer = DarkNormalizer(None)
    data = [np.random.normal(30.0, 10, size=(ny, nx)) for _ in range(6)]
    images = [FakeImage() for _ in range(6)]
    for i, image in enumerate(images):
        image.data = data[i].copy()

    images = [normalizer.run(image) for image in images]

    for i, image in enumerate(images):
        np.testing.assert_allclose(image.data, data[i] / image.exptime, 1e-5)
