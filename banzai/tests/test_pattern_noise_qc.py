import logging

import pytest
import mock
import numpy as np

from banzai.qc import pattern_noise
from banzai.tests.utils import FakeImage, gaussian2d

logger = logging.getLogger('banzai.qc.pattern_noise')


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(200)


def generate_data(ny=1000, nx=1000, has_pattern_noise=False):
    data = 1000.0 + np.random.normal(0.0, 10.0, size=nx*ny).reshape(ny, nx)
    if has_pattern_noise:
        pn = np.empty((ny, nx))
        # Shift the phase on each row to get the vertical line in the FT
        for iy in range(ny):
            pn[iy] = np.sin(np.arange(nx)/0.1 + 0.1*iy)
        # Add Gaussian envelope to give the peak some width
        pn *= 1000.0 * np.exp(-np.pi * (np.arange(nx)-nx/2)**2 / (2*(nx/5)**2))
        data += pn
    return data


def test_no_input_images(set_random_seed):
    detector = pattern_noise.PatternNoiseDetector(None)
    images = detector.do_stage([])
    assert len(images) == 0


def test_pattern_noise_detects_noise_when_it_should(set_random_seed):
    data = generate_data(has_pattern_noise=True)
    detector = pattern_noise.PatternNoiseDetector(None)
    assert detector.check_for_pattern_noise(data)[0]


def test_pattern_noise_does_not_detect_white_noise(set_random_seed):
    data = generate_data()
    detector = pattern_noise.PatternNoiseDetector(None)
    assert detector.check_for_pattern_noise(data)[0] == False


def test_pattern_noise_does_not_detect_stars(set_random_seed):
    data = generate_data()
    for i in range(5):
        x = np.random.uniform(low=0.0, high=100)
        y = np.random.uniform(low=0.0, high=100)
        brightness = np.random.uniform(low=1000., high=5000.)
        data += gaussian2d(data.shape, x, y, brightness, 3.5)
    detector = pattern_noise.PatternNoiseDetector(None)
    assert detector.check_for_pattern_noise(data)[0] == False


def test_pattern_noise_on_2d_image(set_random_seed):
    image = FakeImage()
    image.data = generate_data(has_pattern_noise=True)

    detector = pattern_noise.PatternNoiseDetector(None)
    logger.error = mock.MagicMock()
    detector.do_stage([image])
    assert logger.error.called


def test_trim_edges():
    assert pattern_noise.trim_image_edges(np.zeros((100, 100)), fractional_edge_width=0.25).shape == (50, 50)
    assert pattern_noise.trim_image_edges(np.zeros((100, 100)), fractional_edge_width=0.10).shape == (80, 80)
    assert pattern_noise.trim_image_edges(np.zeros((100, 120)), fractional_edge_width=0.25).shape == (44, 64)


def test_get_2d_power_band(set_random_seed):
    data = np.random.normal(0.0, 10.0, size=(100, 400))
    fft = abs(np.fft.rfft2(data))[37:62, 5:]
    power_band = pattern_noise.get_2d_power_band(data, fractional_band_width=0.25,
                                                 fractional_inner_edge_to_discard=0.025)
    assert power_band.shape == (25, 196)
    np.testing.assert_allclose(power_band, fft)


def test_compute_snr(set_random_seed):
    data = np.random.normal(1000.0, 20.0, size=(500, 100))
    snr = pattern_noise.compute_snr(data)
    assert len(snr) == data.shape[1]-1
    assert all(snr < 5)


def test_get_odd_integer():
    assert pattern_noise.get_odd_integer(1.5) == 3
    assert pattern_noise.get_odd_integer(2) == 3
    assert pattern_noise.get_odd_integer(2.5) == 3
