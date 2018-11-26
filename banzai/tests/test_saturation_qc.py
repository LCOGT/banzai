import numpy as np

from banzai.tests.utils import FakeImage
from banzai.qc import SaturationTest


def test_no_input_images():
    tester = SaturationTest(None)
    images = tester.do_stage([])
    assert len(images) == 0


def test_no_pixels_saturated():
    tester = SaturationTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.header['SATURATE'] = 65535

    images = tester.do_stage(images)
    for image in images:
        assert image.header.get('SATFRAC') == 0.0
    assert len(images) == 6


def test_nonzero_but_no_pixels_saturated():
    tester = SaturationTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.header['SATURATE'] = 65535
        image.data += 5.0

    images = tester.do_stage(images)
    for image in images:
        assert image.header.get('SATFRAC') == 0.0
    assert len(images) == 6


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

    images = tester.do_stage(images)
    for image in images:
        assert image.header.get('SATFRAC') == 0.0
    assert len(images) == 5


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

    images = tester.do_stage(images)
    for image in images:
        assert np.abs(image.header.get('SATFRAC') - 0.02) < 0.001
    assert len(images) == 0


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

    images = tester.do_stage(images)
    for image in images:
        assert np.abs(image.header.get('SATFRAC') - 0.02) < 0.001
    assert len(images) == 6
