import mock
import numpy as np

from banzai.flats import FlatDivider
from banzai.tests.utils import FakeImage, handles_inhomogeneous_set, FakeContext

from banzai.tests.flat_utils import make_context_with_master_flat


def test_null_input_image():
    subtractor = FlatDivider(FakeContext())
    image = subtractor.run(None)
    assert image is None


def test_master_selection_criteria():
    subtractor = FlatDivider(FakeContext())
    assert subtractor.master_selection_criteria == ['ccdsum', 'filter']


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_returns_null_if_ccdsums_are_different(mock_cal):
    handles_inhomogeneous_set(FlatDivider, FakeContext(), 'ccdsum', '1 1')


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_returns_null_if_nx_are_different(mock_cal):
    handles_inhomogeneous_set(FlatDivider, FakeContext(), 'nx', 105)


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_returns_null_if_ny_are_different(mock_cal):
    handles_inhomogeneous_set(FlatDivider, FakeContext(), 'ny', 107)


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_rejects_image_if_no_master_calibration(mock_cal):
    mock_cal.return_value = None
    nx = 101
    ny = 103
    context = make_context_with_master_flat(nx=nx, ny=ny)
    subtractor = FlatDivider(context)
    image = subtractor.do_stage(FakeImage(nx=nx, ny=ny))
    assert image is None


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_does_not_reject_image_if_no_master_calibration_and_calibrations_not_required(mock_cal):
    mock_cal.return_value = None
    nx = 101
    ny = 103
    context = make_context_with_master_flat(nx=nx, ny=ny)
    context.calibrations_not_required = True
    subtractor = FlatDivider(context)
    image = subtractor.do_stage(FakeImage(nx=nx, ny=ny))
    assert image is not None


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_flat_subtraction_is_reasonable(mock_cal):
    mock_cal.return_value = 'test.fits'
    input_flat = 10.0
    flat_variation = 0.01
    input_level = 2000.0
    nx = 101
    ny = 103

    context = make_context_with_master_flat(flat_level=input_flat, master_flat_variation=flat_variation, nx=nx, ny=ny)

    subtractor = FlatDivider(context)
    image = FakeImage(image_multiplier=input_level)
    image = subtractor.do_stage(image)
    assert np.abs(np.mean(image.data) - input_level / input_flat) < 1.0
