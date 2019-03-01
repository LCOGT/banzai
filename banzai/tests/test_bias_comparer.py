import pytest
import mock
import numpy as np

from banzai.tests.bias_utils import make_context_with_master_bias
from banzai.bias import BiasComparer
from banzai.tests.utils import FakeImage, handles_inhomogeneous_set, FakeContext


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(81232385)


def test_null_input_image():
    comparer = BiasComparer(FakeContext())
    image = comparer.run(None)
    assert image is None


def test_master_selection_criteria():
    comparer = BiasComparer(FakeContext())
    assert comparer.master_selection_criteria == ['ccdsum']


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_returns_null_if_ccdsums_are_different(mock_cal):
    handles_inhomogeneous_set(BiasComparer, FakeContext(), 'ccdsum', '1 1')


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_returns_null_if_nx_are_different(mock_cal):
    handles_inhomogeneous_set(BiasComparer, FakeContext(), 'nx', 105)


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_returns_null_if_ny_are_different(mock_cal):
    handles_inhomogeneous_set(BiasComparer, FakeContext(), 'ny', 107)


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_flags_bad_if_no_master_calibration(mock_cal, set_random_seed):
    mock_cal.return_value = None
    nx = 101
    ny = 103
    context = make_context_with_master_bias(nx=nx, ny=ny)
    comparer = BiasComparer(context)
    image = comparer.do_stage(FakeImage(nx=nx, ny=ny))
    assert image.is_bad is True


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_does_not_reject_noisy_image(mock_cal, set_random_seed):
    mock_cal.return_value = 'test.fits'
    master_readnoise = 3.0
    nx = 101
    ny = 103

    context = make_context_with_master_bias(readnoise=master_readnoise, nx=nx, ny=ny)
    comparer = BiasComparer(context)
    image = FakeImage(image_multiplier=0.0)
    image.data = np.random.normal(0.0, image.readnoise, size=(ny, nx))

    image = comparer.do_stage(image)

    assert image.is_bad is False


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_does_flag_bad_image(mock_cal, set_random_seed):
    mock_cal.return_value = 'test.fits'
    master_readnoise = 3.0
    nx = 101
    ny = 103

    context = make_context_with_master_bias(readnoise=master_readnoise, nx=nx, ny=ny)
    comparer = BiasComparer(context)
    image = FakeImage(image_multiplier=0.0)
    image.data = np.random.normal(0.0, image.readnoise, size=(ny, nx))

    x_indexes = np.random.choice(np.arange(nx), size=2000)
    y_indexes = np.random.choice(np.arange(ny), size=2000)
    for x, y in zip(x_indexes, y_indexes):
        image.data[y, x] = np.random.normal(100, image.readnoise)
    image = comparer.do_stage(image)

    assert image.is_bad
