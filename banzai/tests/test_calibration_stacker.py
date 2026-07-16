from banzai.lco import LCOCalibrationFrame
from banzai.data import CCDData
from banzai.context import Context
from banzai.calibrations import CalibrationStacker
from banzai.dbs import Instrument
from banzai.tests.utils import pre_refactor_stack_snapshot
import numpy as np
from astropy.io import fits
import pytest

nx, ny = 102, 105
header = {'DATASEC': f'[1:{nx},1:{ny}]', 'DETSEC': f'[1:{nx},1:{ny}]', 'CCDSUM': '1 1',
          'OBSTYPE': 'TEST', 'RDNOISE': 3.0, 'TELESCOP': '1m0-02', 'DAY-OBS': '20191209',
          'DATE-OBS': '2019-12-09T00:00:00', 'RA': 0.0, 'DEC': 0.0}
context = {'CALIBRATION_MIN_FRAMES': {'TEST': 1},
           'CALIBRATION_FILENAME_FUNCTIONS': {'TEST': ['banzai.utils.file_utils.ccdsum_to_filename']},
           'CALIBRATION_SET_CRITERIA': {'TEST': ['binning']},
           'CALIBRATION_FRAME_CLASS': 'banzai.lco.LCOCalibrationFrame',
           'TELESCOPE_FILENAME_FUNCTION': 'banzai.utils.file_utils.telescope_to_filename',
           'MASTER_CALIBRATION_EXTENSION_ORDER': {'BIAS': ['SCI', 'BPM', 'ERR'],
                                                  'DARK': ['SCI', 'BPM', 'ERR'],
                                                  'SKYFLAT': ['SCI', 'BPM', 'ERR']}}
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
    stacked_data = stage.do_stage(test_images)
    np.testing.assert_allclose(stacked_data.data, np.ones((ny, nx)) * np.mean(np.arange(9)))
    np.testing.assert_allclose(stacked_data.primary_hdu.uncertainty, np.ones((ny, nx)))
    assert np.all(stacked_data.mask == 0)


def test_stacking_with_noise():
    test_images = [LCOCalibrationFrame([CCDData(np.random.normal(0.0, 3.0, size=(ny, nx)),
                                                meta=fits.Header(header))], '')
                   for i in range(81)]
    for image in test_images:
        image.instrument = instrument
    stage = FakeStacker(context)
    stacked_data = stage.do_stage(test_images)
    np.testing.assert_allclose(stacked_data.data, np.zeros((ny, nx)), atol=5.0/3.0)
    np.testing.assert_allclose(stacked_data.primary_hdu.uncertainty, np.ones((ny, nx)) / 3.0, atol=0.05)
    assert np.all(stacked_data.mask == 0)


def test_stacking_with_different_pixels():
    d = np.arange(nx*ny, dtype=np.float64).reshape(ny, nx)
    test_images = [LCOCalibrationFrame([CCDData(d * i, meta=fits.Header(header))], '')
                   for i in range(9)]
    for image in test_images:
        image.instrument = instrument
    stage = FakeStacker(context)
    stacked_data = stage.do_stage(test_images)
    np.testing.assert_allclose(stacked_data.data, d * np.mean(np.arange(9)))
    np.testing.assert_allclose(stacked_data.primary_hdu.uncertainty, np.ones((ny, nx)))
    assert np.all(stacked_data.mask == 0)


def test_calibration_stacker_matches_pre_refactor_snapshot():
    rng = np.random.default_rng(920381)
    test_images = []
    for i in range(7):
        image_data = rng.normal(loc=i, scale=2.0, size=(ny, nx))
        mask = np.zeros((ny, nx), dtype=np.uint8)
        uncertainty = rng.uniform(1.0, 3.0, size=(ny, nx))
        hdu = CCDData(image_data, meta=fits.Header(header), mask=mask, uncertainty=uncertainty)
        test_images.append(LCOCalibrationFrame([hdu], ''))

    test_images[0].primary_hdu.data[2, 3] = 1000.0
    test_images[1].primary_hdu.mask[5, 7] = 1
    test_images[3].primary_hdu.mask[11, 13] = 2
    for image in test_images:
        image.instrument = instrument

    stage = FakeStacker(context)
    stacked_data = stage.do_stage(test_images)
    expected = pre_refactor_stack_snapshot([image.primary_hdu for image in test_images], 3.0)

    assert np.array_equal(stacked_data.data, expected.data)
    assert np.array_equal(stacked_data.primary_hdu.uncertainty, expected.uncertainty)
    assert np.array_equal(stacked_data.mask, expected.mask)
