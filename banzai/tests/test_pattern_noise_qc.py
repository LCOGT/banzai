from __future__ import absolute_import, division, print_function, unicode_literals
import numpy as np
from banzai.qc import pattern_noise
from banzai.tests.utils import FakeImage, gaussian2d
import pytest


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(200)


def test_no_input_images(set_random_seed):
    detector = pattern_noise.PatternNoiseDetector(None)
    images = detector.do_stage([])
    assert len(images) == 0


def test_group_by_keywords(set_random_seed):
    detector = pattern_noise.PatternNoiseDetector(None)
    assert detector.group_by_keywords is None


def test_pattern_noise_detects_noise_when_it_should(set_random_seed):
    data = 100.0 * np.sin(np.arange(1000000) / 0.1) + 1000.0 + np.random.normal(0.0, 10.0, size=1000000)
    data = data.reshape(1000, 1000)
    assert pattern_noise.check_for_pattern_noise(data, 10, 5)


def test_pattern_noise_does_not_detect_white_noise(set_random_seed):
    data = 1000 + np.random.normal(0.0, 10.0, size=1000000)
    data = data.reshape(1000, 1000)
    assert pattern_noise.check_for_pattern_noise(data, 10, 5) == False


def test_pattern_noise_does_not_detect_stars(set_random_seed):
    data = 1000 + np.random.normal(0.0, 10.0, size=1000000)
    data = data.reshape(1000, 1000)
    for i in range(5):
        x = np.random.uniform(low=0.0, high=100)
        y = np.random.uniform(low=0.0, high=100)
        brightness = np.random.uniform(low=1000., high=5000.)
        data += gaussian2d(data.shape, x, y, brightness, 3.5)
    assert pattern_noise.check_for_pattern_noise(data, 10, 5) == False


def test_pattern_noise_on_2d_image(set_random_seed):
    data = 100.0 * np.sin(np.arange(1000000) / 0.1) + 1000.0 + np.random.normal(0.0, 10.0, size=1000000)
    data = data.reshape(1000, 1000)

    image = FakeImage()
    image.data = data

    detector = pattern_noise.PatternNoiseDetector(None)

    assert detector.do_stage([image]) == []


def test_pattern_noise_on_3d_image(set_random_seed):
    data = 100.0 * np.sin(np.arange(1000000 * 4) / 0.1) + 1000.0 + np.random.normal(0.0, 10.0, size=1000000 * 4)
    data = data.reshape(4, 1000, 1000)

    image = FakeImage()
    image.data = data

    detector = pattern_noise.PatternNoiseDetector(None)

    assert detector.do_stage([image]) == []


def test_pattern_noise_in_only_one_quadrant(set_random_seed):
    data = np.random.normal(0.0, 10.0, size=1000000 * 4) + 1000.0
    data = data.reshape(4, 1000, 1000)
    data[3] += 100.0 * np.sin(np.arange(1e6) / 0.1).reshape(1000, 1000)

    image = FakeImage()
    image.data = data

    detector = pattern_noise.PatternNoiseDetector(None)

    assert detector.do_stage([image]) == []
