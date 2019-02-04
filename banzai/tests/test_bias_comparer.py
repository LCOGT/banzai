import pytest
import mock
import numpy as np

from banzai.tests.bias_utils import make_context_with_master_bias
from banzai.bias import BiasComparer
from banzai.tests.utils import FakeImage, throws_inhomogeneous_set_exception, FakeContext


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(81232385)


def test_no_input_images(set_random_seed):
    comparer = BiasComparer(FakeContext())
    images = comparer.do_stage([])
    assert len(images) == 0


def test_master_selection_criteria(set_random_seed):
    comparer = BiasComparer(FakeContext())
    assert comparer.master_selection_criteria == ['ccdsum']


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_ccdsums_are_different(mock_cal, set_random_seed):
    throws_inhomogeneous_set_exception(BiasComparer, FakeContext(), 'ccdsum', '1 1')


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_epochs_are_different(mock_cal, set_random_seed):
    throws_inhomogeneous_set_exception(BiasComparer, FakeContext(), 'epoch', '20160102')


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_nx_are_different(mock_cal, set_random_seed):
    mock_cal.return_value = 'test.fits'
    throws_inhomogeneous_set_exception(BiasComparer, FakeContext(), 'nx', 105)


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_ny_are_different(mock_cal, set_random_seed):
    mock_cal.return_value = 'test.fits'
    throws_inhomogeneous_set_exception(BiasComparer, FakeContext(), 'ny', 107)


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_flags_bad_if_no_master_calibration(mock_cal, set_random_seed):
    mock_cal.return_value = None
    nx = 101
    ny = 103
    context = make_context_with_master_bias(nx=nx, ny=ny)
    comparer = BiasComparer(context)
    images = comparer.do_stage([FakeImage(nx=nx, ny=ny) for x in range(6)])
    assert all([image.is_bad for image in images])


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_does_not_reject_noisy_images(mock_cal, set_random_seed):
    mock_cal.return_value = 'test.fits'
    master_readnoise = 3.0
    nx = 101
    ny = 103

    context = make_context_with_master_bias(readnoise=master_readnoise, nx=nx, ny=ny)
    comparer = BiasComparer(context)
    images = [FakeImage(image_multiplier=0.0) for x in range(6)]
    for image in images:
        image.data = np.random.normal(0.0, image.readnoise, size=(ny, nx))

    images = comparer.do_stage(images)

    assert not any([image.is_bad for image in images])


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_does_flag_bad_images(mock_cal, set_random_seed):
    mock_cal.return_value = 'test.fits'
    master_readnoise = 3.0
    nx = 101
    ny = 103

    context = make_context_with_master_bias(readnoise=master_readnoise, nx=nx, ny=ny)
    comparer = BiasComparer(context)
    images = [FakeImage(image_multiplier=0.0) for x in range(6)]
    for image in images:
        image.data = np.random.normal(0.0, image.readnoise, size=(ny, nx))

    for i in [2, 4]:
        x_indexes = np.random.choice(np.arange(nx), size=2000)
        y_indexes = np.random.choice(np.arange(ny), size=2000)
        for x, y in zip(x_indexes, y_indexes):
            images[i].data[y, x] = np.random.normal(100, images[i].readnoise)
    images = comparer.do_stage(images)

    assert sum([image.is_bad for image in images]) == 2
