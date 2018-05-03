from __future__ import absolute_import, division, print_function, unicode_literals
from banzai.dark import DarkNormalizer
from banzai.tests.utils import FakeImage
import numpy as np
import pytest


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(7298374)


def test_no_input_images(set_random_seed):
    normalizer = DarkNormalizer(None)
    images = normalizer.do_stage([])
    assert len(images) == 0


def test_group_by_keywords(set_random_seed):
    normalizer = DarkNormalizer(None)
    assert normalizer.group_by_keywords is None


def test_dark_normalization_is_reasonable(set_random_seed):
    nx = 101
    ny = 103

    normalizer = DarkNormalizer(None)
    data = [np.random.normal(30.0, 10, size=(ny, nx)) for _ in range(6)]
    images = [FakeImage() for _ in range(6)]
    for i, image in enumerate(images):
        image.data = data[i].copy()

    images = normalizer.do_stage(images)

    for i, image in enumerate(images):
        np.testing.assert_allclose(image.data, data[i] / image.exptime, 1e-5)
