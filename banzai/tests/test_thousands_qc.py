import numpy as np

from banzai.tests.utils import FakeImage
from banzai.qc import ThousandsTest


def test_null_input_image():
    tester = ThousandsTest(None)
    image = tester.run(None)
    assert image is None


def test_no_pixels_1000():
    tester = ThousandsTest(None)
    nx = 101
    ny = 103

    image = tester.run(FakeImage(nx=nx, ny=ny))

    assert image is not None


def test_nonzero_but_no_pixels_1000():
    tester = ThousandsTest(None)
    nx = 101
    ny = 103

    image = FakeImage(nx=nx, ny=ny)
    image.data += 5

    image = tester.do_stage(image)

    assert image is not None


def test_all_1000s():
    tester = ThousandsTest(None)
    nx = 101
    ny = 103

    image = FakeImage(nx=nx, ny=ny)
    image.data[:, :] = 1000

    image = tester.do_stage(image)

    assert image is None


def test_5_percent_1000():
    tester = ThousandsTest(None)
    nx = 101
    ny = 103

    image = FakeImage(nx=nx, ny=ny)
    random_pixels_x = np.random.randint(0, nx - 1, size=int(0.05 * nx * ny))
    random_pixels_y = np.random.randint(0, ny - 1, size=int(0.05 * nx * ny))
    for i in zip(random_pixels_y, random_pixels_x):
        image.data[i] = 1000

    image = tester.do_stage(image)

    assert image is not None


def test_all_image_30_percent_1000():
    tester = ThousandsTest(None)
    nx = 101
    ny = 103

    image = FakeImage(nx=nx, ny=ny)
    random_pixels_x = np.random.randint(0, nx - 1, size=int(0.3 * nx * ny))
    random_pixels_y = np.random.randint(0, ny - 1, size=int(0.3 * nx * ny))
    for i in zip(random_pixels_y, random_pixels_x):
        image.data[i] = 1000

    image = tester.do_stage(image)

    assert image is None
