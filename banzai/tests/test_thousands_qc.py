import numpy as np

from banzai.tests.utils import FakeImage
from banzai.qc import ThousandsTest


def test_no_input_images():
    tester = ThousandsTest(None)
    images = tester.do_stage([])
    assert len(images) == 0


def test_no_pixels_1000():
    tester = ThousandsTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]

    images = tester.do_stage(images)

    assert len(images) == 6


def test_nonzero_but_no_pixels_1000():
    tester = ThousandsTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.data += 5

    images = tester.do_stage(images)

    assert len(images) == 6


def test_1_image_all_1000s():
    tester = ThousandsTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.data += 5

    images[3].data[:, :] = 1000

    images = tester.do_stage(images)

    assert len(images) == 5


def test_all_images_all_1000s():
    tester = ThousandsTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.data[:, :] = 1000

    images = tester.do_stage(images)

    assert len(images) == 0


def test_1_image_5_percent_1000():
    tester = ThousandsTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]

    random_pixels_x = np.random.randint(0, nx - 1, size=int(0.05 * nx * ny))
    random_pixels_y = np.random.randint(0, ny - 1, size=int(0.05 * nx * ny))
    for i in zip(random_pixels_y, random_pixels_x):
        images[3].data[i] = 1000

    images = tester.do_stage(images)

    assert len(images) == 6


def test_all_images_5_percent_1000():
    tester = ThousandsTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        random_pixels_x = np.random.randint(0, nx - 1, size=int(0.05 * nx * ny))
        random_pixels_y = np.random.randint(0, ny - 1, size=int(0.05 * nx * ny))
        for i in zip(random_pixels_y, random_pixels_x):
            image.data[i] = 1000

    images = tester.do_stage(images)

    assert len(images) == 6


def test_1_image_30_percent_1000():
    tester = ThousandsTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    random_pixels_x = np.random.randint(0, nx - 1, size=int(0.3 * nx * ny))
    random_pixels_y = np.random.randint(0, ny - 1, size=int(0.3 * nx * ny))
    for i in zip(random_pixels_y, random_pixels_x):
        images[3].data[i] = 1000

    images = tester.do_stage(images)

    assert len(images) == 5


def test_all_image_30_percent_1000():
    tester = ThousandsTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        random_pixels_x = np.random.randint(0, nx - 1, size=int(0.3 * nx * ny))
        random_pixels_y = np.random.randint(0, ny - 1, size=int(0.3 * nx * ny))
        for i in zip(random_pixels_y, random_pixels_x):
            image.data[i] = 1000

    images = tester.do_stage(images)

    assert len(images) == 0
