import pytest
import numpy as np
from astropy.io.fits import Header

from banzai.bias import BiasMasterLevelSubtractor
from banzai.lco import LCOCalibrationFrame
from banzai.data import CCDData
from banzai.tests.utils import FakeLCOObservationFrame, FakeCCDData

pytestmark = pytest.mark.bias_level_subtractor


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(9723492)


def test_null_input_image():
    subtractor = BiasMasterLevelSubtractor(None)
    image = subtractor.run(None)
    assert image is None


def test_header_has_biaslevel():
    subtractor = BiasMasterLevelSubtractor(None)
    image = FakeLCOObservationFrame()
    image = subtractor.do_stage(image)
    assert 'BIASLVL' in image.meta


def test_header_biaslevel_is_1():
    subtractor = BiasMasterLevelSubtractor(None)
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.ones((100,100)))])
    image = subtractor.do_stage(image)
    assert image.meta.get('BIASLVL') == 1


def test_header_biaslevel_is_2():
    subtractor = BiasMasterLevelSubtractor(None)
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=2*np.ones((100,100)))])
    image = subtractor.do_stage(image)
    assert image.meta.get('BIASLVL') == 2.0


def test_bias_master_level_subtraction_is_reasonable(set_random_seed):
    input_bias = 2000.0
    read_noise = 15.0

    subtractor = BiasMasterLevelSubtractor(None)
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.random.normal(input_bias, read_noise, size=(100, 100)))])
    image = subtractor.do_stage(image)

    np.testing.assert_allclose(np.zeros(image.data.shape), image.data, atol=8 * read_noise)
    np.testing.assert_allclose(image.meta.get('BIASLVL'), input_bias, atol=1.0)
