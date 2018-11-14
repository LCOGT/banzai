import pytest
import mock
import numpy as np

from banzai.dark import DarkComparer
from banzai.tests.utils import FakeImage, throws_inhomogeneous_set_exception, FakeContext


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(6234585)


class FakeDarkImage(FakeImage):
    def __init__(self, exptime, **kwargs):
        self.exptime = exptime
        super(FakeDarkImage, self).__init__(**kwargs)


def test_no_input_images():
    comparer = DarkComparer(None)
    images = comparer.do_stage([])
    assert len(images) == 0


def test_master_selection_criteria():
    comparer = DarkComparer(None)
    assert comparer.master_selection_criteria == ['ccdsum']


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_ccdsums_are_different(mock_cal, mock_images):
    throws_inhomogeneous_set_exception(DarkComparer, FakeContext(), 'ccdsum', '1 1')


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_epochs_are_different(mock_cal, mock_images):
    throws_inhomogeneous_set_exception(DarkComparer, FakeContext(), 'epoch', '20160102')


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_nx_are_different(mock_cal, mock_images):
    mock_cal.return_value = 'test.fits'
    throws_inhomogeneous_set_exception(DarkComparer, FakeContext(), 'nx', 105)


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_ny_are_different(mock_cal, mock_images):
    mock_cal.return_value = 'test.fits'
    throws_inhomogeneous_set_exception(DarkComparer, FakeContext(), 'ny', 107)


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
@mock.patch('banzai.stages.Stage.save_qc_results')
def test_does_not_raise_exception_if_no_master_calibration(mock_save_qc, mock_cal, mock_images):
    mock_cal.return_value = None
    mock_images.return_value = FakeDarkImage(30.0)

    comparer = DarkComparer(FakeContext())
    images = comparer.do_stage([FakeDarkImage(30.0) for x in range(6)])
    assert len(images) == 6


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
@mock.patch('banzai.stages.Stage.save_qc_results')
def test_does_not_reject_noisy_images(mock_save_qc, mock_cal, mock_image, set_random_seed):
    mock_cal.return_value = 'test.fits'
    master_dark_fraction = 0.05
    nx = 101
    ny = 103
    dark_exptime = 900.0

    fake_master_dark = FakeDarkImage(30.0)
    fake_master_dark.data = np.random.normal(0.0, fake_master_dark.readnoise / 10.0, size=(ny, nx)) / dark_exptime

    size = 101 * 103
    n_hot_pixels = int(size*master_dark_fraction)
    xinds = np.random.choice(np.arange(nx), size=n_hot_pixels, replace=True)
    yinds = np.random.choice(np.arange(ny), size=n_hot_pixels, replace=True)
    dark_pattern = np.zeros((ny, nx))
    for x, y in zip(xinds, yinds):
        dark_pattern[y, x] = np.abs(np.random.normal(10, 3))

    master_dark_noise = np.random.poisson(dark_pattern * dark_exptime) - dark_pattern * dark_exptime
    fake_master_dark.data += dark_pattern + master_dark_noise / dark_exptime / 10.0
    mock_image.return_value = fake_master_dark

    comparer = DarkComparer(FakeContext())
    images = [FakeDarkImage(exptime=dark_exptime) for _ in range(6)]
    for image in images:
        image.data = np.random.normal(0.0, image.readnoise, size=(ny, nx))
        image.data += np.random.poisson(dark_pattern * dark_exptime)
        image.data /= image.exptime

    images = comparer.do_stage(images)

    assert len(images) == 6


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
@mock.patch('banzai.stages.Stage.save_qc_results')
def test_does_reject_bad_images(mock_save_qc, mock_cal, mock_image, set_random_seed):
    mock_cal.return_value = 'test.fits'
    master_dark_fraction = 0.05
    nx = 101
    ny = 103
    dark_exptime = 900.0
    dark_level = 10.0

    fake_master_dark = FakeDarkImage(30.0)
    fake_master_dark.data = np.random.normal(dark_level, fake_master_dark.readnoise / 10.0, size=(ny, nx)) / dark_exptime

    size = 101 * 103
    n_hot_pixels = int(size*master_dark_fraction)
    xinds = np.random.choice(np.arange(nx), size=n_hot_pixels, replace=True)
    yinds = np.random.choice(np.arange(ny), size=n_hot_pixels, replace=True)
    dark_pattern = np.zeros((ny, nx))
    for x, y in zip(xinds, yinds):
        dark_pattern[y, x] = np.abs(np.random.normal(10, 3))

    master_dark_noise = np.random.poisson(dark_pattern * dark_exptime) - dark_pattern * dark_exptime
    fake_master_dark.data += dark_pattern + master_dark_noise / dark_exptime / 10.0
    mock_image.return_value = fake_master_dark

    comparer = DarkComparer(FakeContext())
    images = [FakeDarkImage(exptime=dark_exptime) for _ in range(6)]
    for image in images:
        image.data = np.random.normal(dark_level, image.readnoise, size=(ny, nx))
        image.data += np.random.poisson(dark_pattern * dark_exptime)
        image.data /= image.exptime

    for i in [2, 4]:
        # Make 20% of the image 10 times as bright
        xinds = np.random.choice(np.arange(nx), size=int(0.2 * nx * ny), replace=True)
        yinds = np.random.choice(np.arange(ny), size=int(0.2 * nx * ny), replace=True)
        for x, y in zip(xinds, yinds):
            images[i].data[y, x] *= 10.0
    images = comparer.do_stage(images)

    assert len(images) == 4
