import numpy as np

from banzai.tests.utils import FakeImage
from banzai.qc import SaturationTest


def test_null_input_image():
    tester = SaturationTest(None)
    image = tester.run(None)
    assert image is None


def test_no_pixels_saturated():
    tester = SaturationTest(None)
    nx = 101
    ny = 103

    image = FakeImage(nx=nx, ny=ny)
    image.header['SATURATE'] = 65535

    image = tester.do_stage(image)
    assert image is not None
    assert image.header['SATFRAC'][0] == 0.0


def test_nonzero_but_no_pixels_saturated():
    tester = SaturationTest(None)
    nx = 101
    ny = 103

    image = FakeImage(nx=nx, ny=ny)
    image.header['SATURATE'] = 65535
    image.data += 5.0

    image = tester.do_stage(image)
    assert image is not None
    assert image.header['SATFRAC'][0] == 0.0


def test_10_percent_saturated():
    tester = SaturationTest(None)
    nx = 101
    ny = 103

    image = FakeImage(nx=nx, ny=ny)
    image.header['SATURATE'] = 65535
    random_pixels_x = np.random.randint(0, nx - 1, size=int(0.1 * nx * ny))
    random_pixels_y = np.random.randint(0, ny - 1, size=int(0.1 * nx * ny))
    for i in zip(random_pixels_y, random_pixels_x):
        image.data[i] = image.header['SATURATE']

    image = tester.do_stage(image)
    assert image is None


def test_all_images_2_percent_saturated():
    tester = SaturationTest(None)
    nx = 101
    ny = 103

    image = FakeImage(nx=nx, ny=ny)
    image.header['SATURATE'] = 65535
    random_pixels_x = np.random.randint(0, nx - 1, size=int(0.02 * nx * ny))
    random_pixels_y = np.random.randint(0, ny - 1, size=int(0.02 * nx * ny))
    for i in zip(random_pixels_y, random_pixels_x):
        image.data[i] = image.header['SATURATE']

    image = tester.do_stage(image)
    assert image is not None
    assert np.abs(image.header['SATFRAC'][0] - 0.02) < 0.001
