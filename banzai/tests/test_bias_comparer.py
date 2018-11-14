import pytest
import mock
import numpy as np

from banzai.bias import BiasComparer
from banzai.tests.utils import FakeImage, throws_inhomogeneous_set_exception, FakeContext


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(81232385)


class FakeBiasImage(FakeImage):
    def __init__(self, bias_level=0.0):
        super(FakeBiasImage, self).__init__(image_multiplier=bias_level)
        self.header = {'BIASLVL': bias_level}


def test_no_input_images(set_random_seed):
    comparer = BiasComparer(None)
    images = comparer.do_stage([])
    assert len(images) == 0


def test_master_selection_criteria(set_random_seed):
    comparer = BiasComparer(None)
    assert comparer.master_selection_criteria == ['ccdsum']


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_ccdsums_are_different(mock_cal, mock_images, set_random_seed):
    throws_inhomogeneous_set_exception(BiasComparer, FakeContext(), 'ccdsum', '1 1')


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_epochs_are_different(mock_cal, mock_images, set_random_seed):
    throws_inhomogeneous_set_exception(BiasComparer, FakeContext(), 'epoch', '20160102')


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_nx_are_different(mock_cal, mock_images, set_random_seed):
    mock_cal.return_value = 'test.fits'
    throws_inhomogeneous_set_exception(BiasComparer, FakeContext(), 'nx', 105)


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_ny_are_different(mock_cal, mock_images, set_random_seed):
    mock_cal.return_value = 'test.fits'
    throws_inhomogeneous_set_exception(BiasComparer, FakeContext(), 'ny', 107)


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
@mock.patch('banzai.stages.Stage.save_qc_results')
def test_does_not_raise_exception_if_no_master_calibration(mock_save_qc, mock_cal, mock_images, set_random_seed):
    mock_cal.return_value = None
    mock_images.return_value = FakeBiasImage()
    comparer = BiasComparer(FakeContext())
    images = comparer.do_stage([FakeImage() for x in range(6)])
    assert len(images) == 6


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
@mock.patch('banzai.stages.Stage.save_qc_results')
def test_does_not_reject_noisy_images(mock_save_qc, mock_cal, mock_image, set_random_seed):
    mock_cal.return_value = 'test.fits'
    master_readnoise = 3.0
    nx = 101
    ny = 103

    fake_master_bias = FakeBiasImage()
    fake_master_bias.data = np.random.normal(0.0, master_readnoise, size=(ny, nx))
    mock_image.return_value = fake_master_bias

    comparer = BiasComparer(FakeContext())
    images = [FakeImage(image_multiplier=0.0) for x in range(6)]
    for image in images:
        image.data = np.random.normal(0.0, image.readnoise, size=(ny, nx))

    images = comparer.do_stage(images)

    assert len(images) == 6


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
@mock.patch('banzai.stages.Stage.save_qc_results')
def test_does_reject_bad_images(mock_save_qc, mock_cal, mock_image, set_random_seed):
    mock_cal.return_value = 'test.fits'
    master_readnoise = 3.0
    nx = 101
    ny = 103

    fake_master_bias = FakeBiasImage()
    fake_master_bias.data = np.random.normal(0.0, master_readnoise, size=(ny, nx))
    mock_image.return_value = fake_master_bias

    comparer = BiasComparer(FakeContext())
    images = [FakeImage(image_multiplier=0.0) for x in range(6)]
    for image in images:
        image.data = np.random.normal(0.0, image.readnoise, size=(ny, nx))

    for i in [2, 4]:
        x_indexes = np.random.choice(np.arange(nx), size=2000)
        y_indexes = np.random.choice(np.arange(ny), size=2000)
        for x, y in zip(x_indexes, y_indexes):
            images[i].data[y, x] = np.random.normal(100, images[i].readnoise)
    images = comparer.do_stage(images)

    assert len(images) == 4
