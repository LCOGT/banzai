import pytest
import numpy as np

from banzai.tests.utils import FakeLCOObservationFrame, FakeCCDData
from banzai.qc import ThousandsTest

pytestmark = pytest.mark.thousands_qc


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(81232385)


def test_null_input_image():
    tester = ThousandsTest(None)
    image = tester.run(None)
    assert image is None


def test_no_pixels_1000():
    tester = ThousandsTest(None)
    image = tester.do_stage(FakeLCOObservationFrame([FakeCCDData(image_multiplier=0)]))
    assert image is not None


def test_nonzero_but_no_pixels_1000():
    tester = ThousandsTest(None)
    image = FakeLCOObservationFrame([FakeCCDData(image_multiplier=5)])
    image = tester.do_stage(image)
    assert image is not None


def test_image_all_1000s():
    tester = ThousandsTest(None)
    image = FakeLCOObservationFrame([FakeCCDData(image_multiplier=1000)])
    image = tester.do_stage(image)
    assert image is None


def test_image_5_percent_1000(set_random_seed):
    tester = ThousandsTest(None)
    nx = 101
    ny = 103
    data = np.ones((ny, nx))
    random_pixels_x = np.random.randint(0, nx - 1, size=int(0.05 * nx * ny))
    random_pixels_y = np.random.randint(0, ny - 1, size=int(0.05 * nx * ny))
    for i in zip(random_pixels_y, random_pixels_x):
        data[i] = 1000
    image = FakeLCOObservationFrame([FakeCCDData(data=data)])
    image = tester.do_stage(image)
    assert image is not None


def test_image_30_percent_1000(set_random_seed):
    tester = ThousandsTest(None)
    nx = 101
    ny = 103
    data = np.ones((ny, nx))
    random_pixels_x = np.random.randint(0, nx - 1, size=int(0.3 * nx * ny))
    random_pixels_y = np.random.randint(0, ny - 1, size=int(0.3 * nx * ny))
    for i in zip(random_pixels_y, random_pixels_x):
        data[i] = 1000
    image = FakeLCOObservationFrame([FakeCCDData(data=data)])
    image = tester.do_stage(image)
    assert image is None
