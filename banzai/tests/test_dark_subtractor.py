import mock
import numpy as np

from banzai.dark import DarkSubtractor
from banzai.tests.utils import FakeImage, handles_inhomogeneous_set, FakeContext

from banzai.tests.dark_utils import make_context_with_realistic_master_dark, FakeDarkImage


def test_null_input_image():
    subtractor = DarkSubtractor(FakeContext())
    image = subtractor.run(None)
    assert image is None


def test_master_selection_criteria():
    subtractor = DarkSubtractor(FakeContext())
    assert subtractor.master_selection_criteria == ['ccdsum']


@mock.patch('banzai.calibrations.FRAME_CLASS', side_effect=FakeDarkImage)
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_returns_null_if_ccdsums_are_different(mock_cal, mock_frame):
    handles_inhomogeneous_set(DarkSubtractor, FakeContext(), 'ccdsum', '1 1')


@mock.patch('banzai.calibrations.FRAME_CLASS', side_effect=FakeDarkImage)
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_returns_null_if_nx_are_different(mock_cal, mock_frame):
    handles_inhomogeneous_set(DarkSubtractor, FakeContext(), 'nx', 105)


@mock.patch('banzai.calibrations.FRAME_CLASS', side_effect=FakeDarkImage)
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_returns_null_if_ny_are_different(mock_cal, mock_frame):
    handles_inhomogeneous_set(DarkSubtractor, FakeContext(), 'ny', 107)


@mock.patch('banzai.calibrations.FRAME_CLASS', side_effect=FakeDarkImage)
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_rejects_image_if_no_master_calibration(mock_cal, mock_frame):
    mock_cal.return_value = None
    nx = 101
    ny = 103
    context = make_context_with_realistic_master_dark(nx=nx, ny=ny)
    subtractor = DarkSubtractor(context)
    image = subtractor.do_stage(FakeImage(nx=nx, ny=ny))
    assert image is None


@mock.patch('banzai.calibrations.FRAME_CLASS', side_effect=FakeDarkImage)
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_does_not_reject_image_if_no_master_calibration_and_calibrations_not_required(mock_cal, mock_frame):
    mock_cal.return_value = None
    nx = 101
    ny = 103
    context = make_context_with_realistic_master_dark(nx=nx, ny=ny)
    context.calibrations_not_required = True
    subtractor = DarkSubtractor(context)
    image = subtractor.do_stage(FakeImage(nx=nx, ny=ny))
    assert image is not None


@mock.patch('banzai.calibrations.FRAME_CLASS')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_dark_subtraction_is_reasonable(mock_cal, mock_frame):
    mock_cal.return_value = 'test.fits'
    input_dark = 1000.0
    dark_exptime = 900.0
    input_readnoise = 0.0
    n_stacked_images = 100
    input_level = 2000.0
    nx = 101
    ny = 103
    mock_frame.return_value = FakeDarkImage(dark_level=input_dark, readnoise=input_readnoise, nx=nx, ny=ny,
                                            dark_exptime=dark_exptime, n_stacked_images=n_stacked_images)
    subtractor = DarkSubtractor(FakeContext())
    image = FakeImage(image_multiplier=input_level)
    image = subtractor.do_stage(image)
    expected_dark_subtraction = (input_level - (input_dark + input_dark / np.sqrt(n_stacked_images)) / dark_exptime * image.exptime)
    assert np.abs(np.mean(image.data) - expected_dark_subtraction) < 1.0
