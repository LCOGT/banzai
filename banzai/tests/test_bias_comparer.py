import pytest
import mock
import numpy as np

from banzai.bias import BiasComparer
from banzai.tests.utils import FakeContext
from banzai.images import LCOCalibrationFrame, CCDData

pytestmark = pytest.mark.bias_comparer

@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(81232385)


def test_null_input_image():
    comparer = BiasComparer(FakeContext())
    image = comparer.run(None)
    assert image is None


def test_master_selection_criteria():
    comparer = BiasComparer(FakeContext())
    assert comparer.master_selection_criteria == ['configuration_mode', 'binning']


@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_filename')
def test_flags_bad_if_no_master_calibration(mock_filename, set_random_seed):
    fake_image = LCOCalibrationFrame(hdu_list=[CCDData(data=np.zeros(0),
                                                       meta={'configuration_mode': 'full_frame'})],
                                     file_path='test1.fits')
    mock_filename.return_value = None
    comparer = BiasComparer(FakeContext())
    image = comparer.do_stage(fake_image)
    assert image.is_bad is True


@mock.patch('banzai.images.LCOFrameFactory.open')
@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_filename')
def test_does_not_reject_noisy_image(mock_master_cal_name, mock_master_frame, set_random_seed):
    mock_master_cal_name.return_value = 'test.fits'
    master_readnoise = 3.0
    image_readnoise = 11.0
    nx = 101
    ny = 103

    fake_master_image = LCOCalibrationFrame(hdu_list=[CCDData(data=np.random.normal(0.0, master_readnoise, size=(ny, nx)),
                                                       meta={'RDNOISE': master_readnoise})],
                                            file_path='test1.fits')
    fake_master_image.is_master = True
    mock_master_frame.return_value = fake_master_image

    comparer = BiasComparer(FakeContext())
    fake_image = LCOCalibrationFrame(hdu_list=[CCDData(data=np.random.normal(0.0, image_readnoise, size=(ny, nx)),
                                                       meta={'RDNOISE': image_readnoise})],
                                     file_path='test2.fits')
    image = comparer.do_stage(fake_image)

    assert image.is_bad is False


@mock.patch('banzai.images.LCOFrameFactory.open')
@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_filename')
def test_does_flag_bad_image(mock_master_cal_name, mock_master_frame, set_random_seed):
    mock_master_cal_name.return_value = 'test.fits'
    master_readnoise = 3.0
    image_readnoise = 11.0
    nx = 101
    ny = 103

    fake_master_image = LCOCalibrationFrame(hdu_list=[CCDData(data=np.random.normal(0.0, master_readnoise, size=(ny, nx)),
                                                       meta={'RDNOISE': master_readnoise,
                                                             'BIASLVL': 0.0 })],
                                            file_path='test1.fits')
    mock_master_frame.return_value = fake_master_image
    comparer = BiasComparer(FakeContext())
    fake_image = LCOCalibrationFrame(hdu_list=[CCDData(data=np.random.normal(0.0, image_readnoise, size=(ny, nx)),
                                                       meta={'RDNOISE': image_readnoise,
                                                             'BIASLVL': 0.0})],
                                     file_path='test2.fits')

    x_indexes = np.random.choice(np.arange(nx), size=2000)
    y_indexes = np.random.choice(np.arange(ny), size=2000)
    for x, y in zip(x_indexes, y_indexes):
        fake_image.data[y, x] = np.random.normal(100, image_readnoise)
    image = comparer.do_stage(fake_image)

    assert image.is_bad
