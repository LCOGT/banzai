import pytest
import numpy as np
import mock

from banzai.dark import DarkSubtractor
from banzai.tests.utils import FakeCCDData, FakeLCOObservationFrame, FakeContext
from banzai.lco import LCOCalibrationFrame
from banzai.data import CCDData


pytestmark = pytest.mark.dark_subtractor


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(7298374)


def test_null_input_images():
    normalizer = DarkSubtractor(None)
    image = normalizer.run(None)
    assert image is None


@mock.patch('banzai.lco.LCOFrameFactory.open')
@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_file_info', return_value='test.fits')
def test_reasonable_dark_subtraction(mock_super_cal_name, mock_super_frame):

    mock_super_cal_name.return_value = {'filename': 'test.fits'}
    mock_super_frame.return_value = LCOCalibrationFrame(hdu_list=[CCDData(data=0.5*np.ones((100,100)),
                                                                          meta={'EXPTIME': 1.0,
                                                                                'SATURATE': 35000,
                                                                                'GAIN': 1.0,
                                                                                'MAXLIN': 35000,
                                                                                'ISMASTER': True,
                                                                                'CCDATEMP': -100})], file_path='/tmp')
    image = FakeLCOObservationFrame(hdu_list=[CCDData(data=4*np.ones((100,100)), meta={'EXPTIME': 2.0,
                                                                                        'SATURATE': 35000,
                                                                                        'GAIN': 1.0,
                                                                                        'MAXLIN': 35000,
                                                                                        'CCDATEMP': -100})])
    subtractor = DarkSubtractor(FakeContext())
    image = subtractor.do_stage(image)
    np.testing.assert_allclose(image.data, 3*np.ones((100,100)))


@mock.patch('banzai.lco.LCOFrameFactory.open')
@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_file_info', return_value='test.fits')
def test_reasonable_dark_subtraction_with_scaling(mock_super_cal_name, mock_super_frame):
    dark_temperature_coefficient = 0.5
    image_measured_temp = 0
    super_measured_temp = -5

    mock_super_cal_name.return_value = {'filename': 'test.fits'}
    mock_super_frame.return_value = LCOCalibrationFrame(hdu_list=[CCDData(data=0.5*np.ones((100,100)),
                                                                          meta={'EXPTIME': 1.0,
                                                                                'SATURATE': 35000,
                                                                                'GAIN': 1.0,
                                                                                'MAXLIN': 35000,
                                                                                'ISMASTER': True,
                                                                                'DARKCOEF': dark_temperature_coefficient,
                                                                                'CCDATEMP': -5})], file_path='/tmp')
    image = FakeLCOObservationFrame(hdu_list=[CCDData(data=4*np.ones((100,100)),
                                                      meta={'EXPTIME': 2.0,
                                                            'SATURATE': 35000,
                                                            'GAIN': 1.0,
                                                            'MAXLIN': 35000,
                                                            'CCDATEMP': 0})])

    dark_scaling_factor = np.exp(dark_temperature_coefficient * (image_measured_temp - super_measured_temp))
    subtracted_data = image.data - np.ones((100,100)) * dark_scaling_factor
    subtractor = DarkSubtractor(FakeContext())
    image = subtractor.do_stage(image)
    np.testing.assert_allclose(image.data, subtracted_data)
