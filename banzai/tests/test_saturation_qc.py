import pytest
import numpy as np
from astropy.io.fits import Header

from banzai.tests.utils import FakeLCOObservationFrame, FakeCCDData
from banzai.qc import SaturationTest

pytestmark = pytest.mark.saturation_qc


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(81232385)


def test_null_input_image():
    tester = SaturationTest(None)
    image = tester.run(None)
    assert image is None


def test_no_pixels_saturated():
    tester = SaturationTest(None)
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta=Header({'SATURATE': 65535}))])
    image = tester.do_stage(image)
    assert image is not None
    assert image.meta.get('SATFRAC') == 0.0


def test_nonzero_but_no_pixels_saturated():
    tester = SaturationTest(None)
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta=Header({'SATURATE': 65535}))])
    image.primary_hdu.data += 5.0
    image = tester.do_stage(image)
    assert image is not None
    assert image.meta.get('SATFRAC') == 0.0


def test_image_10_percent_saturated_rejected(set_random_seed):
    tester = SaturationTest(None)
    nx = 101
    ny = 103
    saturation_value = 65535

    random_pixels_x = np.random.randint(0, nx - 1, size=int(0.1 * nx * ny))
    random_pixels_y = np.random.randint(0, ny - 1, size=int(0.1 * nx * ny))
    image_data = np.ones((ny, nx))

    for i in zip(random_pixels_y, random_pixels_x):
        image_data[i] = saturation_value

    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=image_data,
                                                          meta={'SATURATE': 65535})])

    image = tester.do_stage(image)
    assert image is None


def test_image_2_percent_saturated(set_random_seed):
    tester = SaturationTest(None)
    nx = 101
    ny = 103
    saturation_value = 65535

    random_pixels_x = np.random.randint(0, nx - 1, size=int(0.02 * nx * ny))
    random_pixels_y = np.random.randint(0, ny - 1, size=int(0.02 * nx * ny))
    image_data = np.ones((ny, nx))

    for i in zip(random_pixels_y, random_pixels_x):
        image_data[i] = saturation_value

    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=image_data,
                                                          meta=Header({'SATURATE': 65535}))])

    image = tester.do_stage(image)
    assert image is not None
    assert np.abs(image.meta.get('SATFRAC') - 0.02) < 0.001

