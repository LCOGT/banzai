from banzai.lco import LCOCalibrationFrame
from banzai.data import CCDData
from banzai.context import Context
from banzai.calibrations import CalibrationStacker
from banzai.dbs import Instrument
import numpy as np
from astropy.io import fits
import pytest

nx, ny = 102, 105
header = {'DATASEC': f'[1:{nx},1:{ny}]', 'DETSEC': f'[1:{nx},1:{ny}]', 'CCDSUM': '1 1',
          'OBSTYPE': 'TEST', 'RDNOISE': 3.0, 'TELESCOP': '1m0-02', 'DAY-OBS': '20191209',
          'DATE-OBS': '2019-12-09T00:00:00'}
context = {'CALIBRATION_MIN_FRAMES': {'TEST': 1},
           'CALIBRATION_FILENAME_FUNCTIONS': {'TEST': ['banzai.utils.file_utils.ccdsum_to_filename']},
           'CALIBRATION_SET_CRITERIA': {'TEST': ['binning']},
           'MASTER_CALIBRATION_FRAME_CLASS': 'banzai.lco.LCOMasterCalibrationFrame',
           'TELESCOPE_FILENAME_FUNCTION': 'banzai.utils.file_utils.telescope_to_filename'}
context = Context(context)
instrument = Instrument(site='cpt', camera='fa11', name='fa11')


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(84651611)


class FakeStacker(CalibrationStacker):
    @property
    def calibration_type(self):
        return 'TEST'


def test_stacking():
    test_images = [LCOCalibrationFrame([CCDData(np.ones((ny, nx)) * i, meta=fits.Header(header))], '')
                   for i in range(9)]
    for image in test_images:
        image.instrument = instrument
    stage = FakeStacker(context)
    stacked_data = stage.do_stage(test_images)[0]
    np.testing.assert_allclose(stacked_data.data, np.ones((ny, nx)) * np.mean(np.arange(9)))
    np.testing.assert_allclose(stacked_data.primary_hdu.uncertainty, np.ones((ny, nx)))
    assert np.all(stacked_data.mask == 0)


def test_stacking_with_noise():
    test_images = [LCOCalibrationFrame([CCDData(np.random.normal(0.0, 3.0, size=(ny, nx)), meta=fits.Header(header))], '')
                   for i in range(81)]
    for image in test_images:
        image.instrument = instrument
    stage = FakeStacker(context)
    stacked_data = stage.do_stage(test_images)[0]
    np.testing.assert_allclose(stacked_data.data, np.zeros((ny, nx)), atol=5.0/3.0)
    np.testing.assert_allclose(stacked_data.primary_hdu.uncertainty, np.ones((ny, nx)) / 3.0, atol=0.05)
    assert np.all(stacked_data.mask == 0)


def test_stacking_with_different_pixels():
    d = np.arange(nx*ny, dtype=np.float).reshape(ny, nx)
    test_images = [LCOCalibrationFrame([CCDData(d * i, meta=fits.Header(header))], '')
                   for i in range(9)]
    for image in test_images:
        image.instrument = instrument
    stage = FakeStacker(context)
    stacked_data = stage.do_stage(test_images)[0]
    np.testing.assert_allclose(stacked_data.data, d * np.mean(np.arange(9)))
    np.testing.assert_allclose(stacked_data.primary_hdu.uncertainty, np.ones((ny, nx)))
    assert np.all(stacked_data.mask == 0)
