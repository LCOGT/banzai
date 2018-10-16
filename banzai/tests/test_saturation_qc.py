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

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.header['SATURATE'] = 65535

    images = [tester.run(image) for image in images]
    for image in images:
        assert image.header['SATFRAC'][0] == 0.0


def test_nonzero_but_no_pixels_saturated():
    tester = SaturationTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.header['SATURATE'] = 65535
        image.data += 5.0

    images = [tester.run(image) for image in images]
    for image in images:
        assert image.header['SATFRAC'][0] == 0.0


def test_1_image_10_percent_saturated():
    tester = SaturationTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.header['SATURATE'] = 65535

    image = images[3]
    image.header['SATURATE'] = 65535
    random_pixels_x = np.random.randint(0, nx - 1, size=int(0.1 * nx * ny))
    random_pixels_y = np.random.randint(0, ny - 1, size=int(0.1 * nx * ny))

    for i in zip(random_pixels_y, random_pixels_x):
        image.data[i] = image.header['SATURATE']

    images = [tester.run(image) for image in images]
    for image in images:
        if image is not None:
            assert image.header['SATFRAC'][0] == 0.0
    assert images.count(None) == 1


def test_all_images_10_percent_saturated():
    tester = SaturationTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.header['SATURATE'] = 65535
        random_pixels_x = np.random.randint(0, nx - 1, size=int(0.1 * nx * ny))
        random_pixels_y = np.random.randint(0, ny - 1, size=int(0.1 * nx * ny))
        for i in zip(random_pixels_y, random_pixels_x):
            image.data[i] = image.header['SATURATE']

    images = [tester.run(image) for image in images]
    assert images.count(None) == 6


def test_all_images_2_percent_saturated():
    tester = SaturationTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.header['SATURATE'] = 65535
        random_pixels_x = np.random.randint(0, nx - 1, size=int(0.02 * nx * ny))
        random_pixels_y = np.random.randint(0, ny - 1, size=int(0.02 * nx * ny))
        for i in zip(random_pixels_y, random_pixels_x):
            image.data[i] = image.header['SATURATE']

    images = [tester.run(image) for image in images]
    for image in images:
        assert np.abs(image.header['SATFRAC'][0] - 0.02) < 0.001
