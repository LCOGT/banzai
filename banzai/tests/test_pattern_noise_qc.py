from __future__ import absolute_import, division, print_function, unicode_literals
import numpy as np
import mock

from banzai.qc import pattern_noise

from banzai.tests.utils import FakeImage, gaussian2d

np.random.seed(200)


def test_no_input_images():
    detector = pattern_noise.PatternNoiseDetector(None)
    images = detector.do_stage([])
    assert len(images) == 0


def test_group_by_keywords():
    detector = pattern_noise.PatternNoiseDetector(None)
    assert detector.group_by_keywords is None


def test_pattern_noise_detects_noise_when_it_should():
    data = 100.0 * np.sin(np.arange(1000000) / 0.1) + 1000.0 + np.random.normal(0.0, 10.0, size=1000000)
    data = data.reshape(1000, 1000)
    assert pattern_noise.check_for_pattern_noise(data, 10, 5)


def test_pattern_noise_does_not_detect_white_noise():
    data = 1000 + np.random.normal(0.0, 10.0, size=1000000)
    data = data.reshape(1000, 1000)
    assert pattern_noise.check_for_pattern_noise(data, 10, 5) == False


def test_pattern_noise_does_not_detect_stars():
    data = 1000 + np.random.normal(0.0, 10.0, size=1000000)
    data = data.reshape(1000, 1000)
    for i in range(5):
        x = np.random.uniform(low=0.0, high=100)
        y = np.random.uniform(low=0.0, high=100)
        brightness = np.random.uniform(low=1000., high=5000.)
        data += gaussian2d(data.shape, x, y, brightness, 3.5)
    assert pattern_noise.check_for_pattern_noise(data, 10, 5) == False


@mock.patch('banzai.qc.pattern_noise.save_qc_results')
def test_pattern_noise_on_2d_image(mock_save_qc):
    data = 100.0 * np.sin(np.arange(1e6) / 0.1) + 1000.0 + np.random.normal(0.0, 10.0, size=1e6)
    data = data.reshape(1000, 1000)

    image = FakeImage()
    image.data = data

    detector = pattern_noise.PatternNoiseDetector(None)
    _ = detector.do_stage([image])

    assert mock_save_qc.called


@mock.patch('banzai.qc.pattern_noise.save_qc_results')
def test_pattern_noise_on_3d_image(mock_save_qc):
    data = 100.0 * np.sin(np.arange(1e6 * 4) / 0.1) + 1000.0 + np.random.normal(0.0, 10.0, size=1e6 * 4)
    data = data.reshape(4, 1000, 1000)

    image = FakeImage()
    image.data = data

    detector = pattern_noise.PatternNoiseDetector(None)
    _ = detector.do_stage([image])

    assert mock_save_qc.called



@mock.patch('banzai.qc.pattern_noise.save_qc_results', return_value=None)
def test_pattern_noise_in_only_one_quadrant(mock_save_qc):
    data = np.random.normal(0.0, 10.0, size=1e6 * 4) + 1000.0
    data = data.reshape(4, 1000, 1000)
    data[3] += 100.0 * np.sin(np.arange(1e6) / 0.1).reshape(1000, 1000)

    image = FakeImage()
    image.data = data

    detector = pattern_noise.PatternNoiseDetector(None)
    _ = detector.do_stage([image])

    assert mock_save_qc.called
