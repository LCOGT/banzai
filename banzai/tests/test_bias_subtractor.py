import mock
import numpy as np

from banzai.bias import BiasSubtractor
from banzai.tests.utils import FakeImage, handles_inhomogeneous_set, FakeContext

from banzai.tests.bias_utils import make_context_with_master_bias


def test_null_input_image():
    subtractor = BiasSubtractor(FakeContext())
    image = subtractor.run(None)
    assert image is None


def test_master_selection_criteria():
    subtractor = BiasSubtractor(FakeContext())
    assert subtractor.master_selection_criteria == ['ccdsum']


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_header_has_biaslevel(mock_cal):
    nx = 101
    ny = 103
    context = make_context_with_master_bias(nx=nx, ny=ny)
    subtractor = BiasSubtractor(context)
    image = subtractor.do_stage(FakeImage(nx=nx, ny=ny))
    assert image.header.get('BIASLVL') == 0.0


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_header_biaslevel_is_1(mock_cal):
    nx = 101
    ny = 103
    context = make_context_with_master_bias(bias_level=1.0, readnoise=10.0, nx=nx, ny=ny)
    subtractor = BiasSubtractor(context)
    image = subtractor.do_stage(FakeImage(nx=nx, ny=ny))
    assert image.header.get('BIASLVL') == 1.0


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_header_biaslevel_is_2(mock_cal):
    nx = 101
    ny = 103
    context = make_context_with_master_bias(bias_level=2.0, readnoise=10.0, nx=nx, ny=ny)
    subtractor = BiasSubtractor(context)
    image = subtractor.do_stage(FakeImage(nx=nx, ny=ny))
    assert image.header.get('BIASLVL') == 2.0


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_returns_null_if_ccdsums_are_different(mock_cal):
    handles_inhomogeneous_set(BiasSubtractor, FakeContext(), 'ccdsum', '1 1')


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_returns_null_if_nx_are_different(mock_cal):
    handles_inhomogeneous_set(BiasSubtractor, FakeContext(), 'nx', 105)


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_returns_null_if_ny_are_different(mock_cal):
    handles_inhomogeneous_set(BiasSubtractor, FakeContext(), 'ny', 107)


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_rejects_image_if_no_master_calibration(mock_cal):
    mock_cal.return_value = None
    nx = 101
    ny = 103
    context = make_context_with_master_bias(nx=nx, ny=ny)
    subtractor = BiasSubtractor(context)
    image = subtractor.do_stage(FakeImage(nx=nx, ny=ny))
    assert image is None


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_does_not_reject_image_if_no_master_calibration_and_calibrations_not_required(mock_cal):
    mock_cal.return_value = None
    nx = 101
    ny = 103
    context = make_context_with_master_bias(nx=nx, ny=ny)
    context.calibrations_not_required = True
    subtractor = BiasSubtractor(context)
    image = subtractor.do_stage(FakeImage(nx=nx, ny=ny))
    assert image is not None


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_bias_subtraction_is_reasonable(mock_cal):
    mock_cal.return_value = 'test.fits'
    input_bias = 1000.0
    input_readnoise = 9.0
    input_level = 2000.0
    nx = 101
    ny = 103

    context = make_context_with_master_bias(bias_level=input_bias, readnoise=input_readnoise, nx=nx, ny=ny)
    subtractor = BiasSubtractor(context)
    image = FakeImage(image_multiplier=input_level)
    image = subtractor.do_stage(image)
    assert np.abs(image.header.get('BIASLVL') - input_bias) < 1.0
    assert np.abs(np.mean(image.data) - (input_level - input_bias)) < 1.0
