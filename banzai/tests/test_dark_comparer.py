import pytest
import mock
import numpy as np

from banzai.dark import DarkComparer
from banzai.tests.utils import handles_inhomogeneous_set, FakeContext
from banzai.tests.dark_utils import FakeDarkImage, make_context_with_realistic_master_dark, get_dark_pattern


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(6234585)


def test_null_input_image():
    comparer = DarkComparer(FakeContext())
    image = comparer.run(None)
    assert image is None


def test_master_selection_criteria():
    comparer = DarkComparer(FakeContext())
    assert comparer.master_selection_criteria == ['ccdsum']


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_ccdsums_are_different(mock_cal):
    handles_inhomogeneous_set(DarkComparer, FakeContext(), 'ccdsum', '1 1')


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_nx_are_different(mock_cal):
    handles_inhomogeneous_set(DarkComparer, FakeContext(), 'nx', 105)


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_ny_are_different(mock_cal):
    handles_inhomogeneous_set(DarkComparer, FakeContext(), 'ny', 107)


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_flags_bad_if_no_master_calibration(mock_cal):
    mock_cal.return_value = None
    context = FakeContext()
    context.FRAME_CLASS = FakeDarkImage
    comparer = DarkComparer(context)
    image = comparer.do_stage(FakeDarkImage(30.0))
    assert image.is_bad is True


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_does_not_flag_noisy_images(mock_cal, set_random_seed):
    mock_cal.return_value = 'test.fits'
    master_dark_fraction = 0.05
    nx = 101
    ny = 103
    dark_exptime = 900.0

    dark_pattern = get_dark_pattern(nx, ny, master_dark_fraction)
    context = make_context_with_realistic_master_dark(dark_pattern, nx=nx, ny=ny,
                                                      dark_level=30.0, dark_exptime=dark_exptime)
    comparer = DarkComparer(context)
    image = FakeDarkImage(exptime=dark_exptime)
    image.data = np.random.normal(0.0, image.readnoise, size=(ny, nx))
    image.data += np.random.poisson(context.dark_pattern * dark_exptime)
    image.data /= image.exptime

    image = comparer.do_stage(image)

    assert image.is_bad is False


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_does_flag_bad_images( mock_cal, set_random_seed):
    mock_cal.return_value = 'test.fits'
    master_dark_fraction = 0.05
    nx = 101
    ny = 103
    dark_exptime = 900.0
    dark_level = 10.0

    dark_pattern = get_dark_pattern(nx, ny, master_dark_fraction)
    context = make_context_with_realistic_master_dark(dark_pattern, nx=nx, ny=ny, dark_level=30.0,
                                                      dark_exptime=dark_exptime)
    comparer = DarkComparer(context)
    image = FakeDarkImage(exptime=dark_exptime)
    image.data = np.random.normal(dark_level, image.readnoise, size=(ny, nx))
    image.data += np.random.poisson(dark_pattern * dark_exptime)
    image.data /= image.exptime

    # Make 20% of the image 10 times as bright
    xinds = np.random.choice(np.arange(nx), size=int(0.2 * nx * ny), replace=True)
    yinds = np.random.choice(np.arange(ny), size=int(0.2 * nx * ny), replace=True)
    for x, y in zip(xinds, yinds):
        image.data[y, x] *= 10.0
    image = comparer.do_stage(image)

    assert image.is_bad is True
