import mock
import numpy as np
import pytest

from banzai.bias import BiasSubtractor
from banzai.tests.utils import FakeContext, FakeLCOObservationFrame, FakeCCDData

pytestmark = pytest.mark.bias_subtractor


def test_null_input_image():
    subtractor = BiasSubtractor(FakeContext())
    image = subtractor.run(None)
    assert image is None


def test_master_selection_criteria():
    subtractor = BiasSubtractor(FakeContext())
    assert subtractor.master_selection_criteria == ['configuration_mode', 'binning']


@mock.patch('banzai.lco.LCOFrameFactory.open')
@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_file_info', return_value='test.fits')
def test_header_has_biaslevel(mock_master_cal_name, mock_master_frame):
    mock_master_cal_name.return_value = {'filename': 'test.fits'}
    fake_master_image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(bias_level=0.0)])
    mock_master_frame.return_value = fake_master_image
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(bias_level=1.0)])
    subtractor = BiasSubtractor(FakeContext())
    image = subtractor.do_stage(image)
    assert image.meta.get('BIASLVL') == 0.0


@mock.patch('banzai.lco.LCOFrameFactory.open')
@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_file_info', return_value='test.fits')
def test_header_biaslevel_is_1(mock_master_cal_name, mock_master_frame):
    mock_master_cal_name.return_value = {'filename': 'test.fits'}
    mock_master_frame.return_value = FakeLCOObservationFrame(hdu_list=[FakeCCDData(bias_level=1.0, read_noise=10.0)])
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(bias_level=0.0)])
    subtractor = BiasSubtractor(FakeContext())
    image = subtractor.do_stage(image)
    assert image.meta.get('BIASLVL') == 1.0


@mock.patch('banzai.lco.LCOFrameFactory.open')
@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_file_info', return_value='test.fits')
def test_header_biaslevel_is_2(mock_master_cal_name, mock_master_frame):
    mock_master_cal_name.return_value = {'filename': 'test.fits'}
    mock_master_frame.return_value = FakeLCOObservationFrame(hdu_list=[FakeCCDData(bias_level=2.0, read_noise=10.0)])
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(bias_level=0.0)])
    subtractor = BiasSubtractor(FakeContext())
    image = subtractor.do_stage(image)
    assert image.meta.get('BIASLVL') == 2.0


@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_file_info', return_value=None)
def test_flags_image_if_no_master_calibration(mock_cal):
    subtractor = BiasSubtractor(FakeContext(override_missing=False))
    image = subtractor.do_stage(FakeLCOObservationFrame(hdu_list=[FakeCCDData()]))
    assert image is None


@mock.patch('banzai.lco.LCOFrameFactory.open')
@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_file_info')
def test_bias_subtraction_is_reasonable(mock_master_cal_name, mock_master_frame):
    mock_master_cal_name.return_value = {'filename': 'test.fits'}
    input_bias = 1000.0
    input_readnoise = 9.0
    input_level = 2000.0
    nx = 101
    ny = 103
    stacked_hdu = FakeCCDData(bias_level=input_bias, data=np.random.normal(0.0, input_readnoise, size=(ny, nx)))
    mock_master_frame.return_value = FakeLCOObservationFrame(hdu_list=[stacked_hdu])
    subtractor = BiasSubtractor(FakeContext())
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(image_multiplier=input_level)])
    image = subtractor.do_stage(image)
    assert np.abs(image.meta.get('BIASLVL') - input_bias) < 1.0
    assert np.abs(np.mean(image.data) - (input_level - input_bias)) < 1.0


@mock.patch('banzai.lco.LCOFrameFactory.open')
@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_file_info')
def test_multiread_bias_subtraction(mock_super_cal_name, mock_super_frame):
    mock_super_cal_name.return_value = {'filename': 'test.fits'}
    input_bias = 500.0
    nreads = 5
    nx = 101
    ny = 103
    bias_pattern = np.random.normal(0.0, 15.0, size=(ny, nx))
    stacked_hdu = FakeCCDData(bias_level=input_bias, data=bias_pattern)
    mock_super_frame.return_value = FakeLCOObservationFrame(hdu_list=[stacked_hdu])
    data = bias_pattern * nreads + input_bias * nreads
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(bias_level=input_bias, data=data)])
    image.n_sub_exposures = nreads
    subtractor = BiasSubtractor(FakeContext())
    image = subtractor.do_stage(image)
    assert np.abs(image.meta.get('BIASLVL') - input_bias) < 1.0
    assert np.abs(np.mean(image.data)) < 1.0
