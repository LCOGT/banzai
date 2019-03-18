import pytest
import numpy as np

from banzai.tests.utils import FakeImage
from banzai.qc import ZerosTest


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(81232385)


def test_null_input_image():
    tester = ZerosTest(None)
    image = tester.run(None)
    assert image is None


def test_no_pixels_0():
    tester = ZerosTest(None)
    image = tester.do_stage(FakeImage())
    assert image is not None


def test_nonzero_but_no_pixels_0():
    tester = ZerosTest(None)
    image = FakeImage()
    image.data += 5
    image = tester.do_stage(image)
    assert image is not None


def test_image_all_0s():
    tester = ZerosTest(None)
    image = FakeImage()
    image.data += 5
    image.data[:, :] = 0
    image = tester.do_stage(image)
    assert image is None


def test_image_95_percent_0(set_random_seed):
    tester = ZerosTest(None)
    nx = 101
    ny = 103
    image = FakeImage(nx=nx, ny=ny)
    random_pixels_x = np.random.randint(0, nx - 1, size=int(0.95 * nx * ny))
    random_pixels_y = np.random.randint(0, ny - 1, size=int(0.95 * nx * ny))
    for i in zip(random_pixels_y, random_pixels_x):
        image.data[i] = 0
    image = tester.do_stage(image)
    assert image is not None
