import pytest
import numpy as np

from banzai.tests.utils import FakeImage
from banzai.qc import ZerosTest


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(81232385)


def get_random_pixel_pairs(nx, ny, fraction):
    return np.unravel_index(np.random.choice(range(nx * ny), size=int(fraction * nx * ny)), (ny, nx))


def test_null_input_image():
    tester = ZerosTest(None)
    image = tester.run(None)
    assert image is None


def test_no_pixels_0_not_rejected():
    tester = ZerosTest(None)
    image = tester.do_stage(FakeImage(image_multiplier=5))
    assert image is not None


def test_image_all_0s_rejected():
    tester = ZerosTest(None)
    image = FakeImage()
    image.data[:, :] = 0
    image = tester.do_stage(image)
    assert image is None


def test_image_50_percent_0_not_rejected(set_random_seed):
    tester = ZerosTest(None)
    nx = 101
    ny = 103
    image = FakeImage(nx=nx, ny=ny)
    random_pixels = get_random_pixel_pairs(nx, ny, 0.5)
    for j,i in zip(random_pixels[0], random_pixels[1]):
        image.data[i,i] = 0
    image = tester.do_stage(image)
    assert image is not None


def test_image_99_percent_0_rejected(set_random_seed):
    tester = ZerosTest(None)
    nx = 101
    ny = 103
    image = FakeImage(nx=nx, ny=ny)
    random_pixels = get_random_pixel_pairs(nx, ny, 0.99)
    for j,i in zip(random_pixels[0], random_pixels[1]):
        image.data[i] = 0
    image = tester.do_stage(image)
    assert image is None


def test_single_amp_99_percent_0_rejected(set_random_seed):
    tester = ZerosTest(None)
    nx = 101
    ny = 103
    n_amps = 4
    image = FakeImage(nx=nx, ny=ny, n_amps=n_amps)
    random_pixels = get_random_pixel_pairs(nx, ny, 0.99)
    for j,i in zip(random_pixels[0], random_pixels[1]):
        image.data[0][i] = 0
    image = tester.do_stage(image)
    assert image is None
