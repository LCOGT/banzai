from __future__ import absolute_import, division, print_function, unicode_literals
from banzai.bias import BiasComparer
from banzai.tests.utils import FakeImage, throws_inhomogeneous_set_exception, FakeContext
import mock

import numpy as np

np.random.seed(81232385)


def test_no_input_images():
    comparer = BiasComparer(None)
    images = comparer.do_stage([])
    assert len(images) == 0


def test_group_by_keywords():
    comparer = BiasComparer(None)
    assert comparer.group_by_keywords == ['ccdsum']


@mock.patch('banzai.stages.Image')
@mock.patch('banzai.stages.ApplyCalibration.get_calibration_filename')
def test_raises_an_exection_if_ccdsums_are_different(mock_cal, mock_images):
    throws_inhomogeneous_set_exception(BiasComparer, None, 'ccdsum', '1 1')


@mock.patch('banzai.stages.Image')
@mock.patch('banzai.stages.ApplyCalibration.get_calibration_filename')
def test_raises_an_exection_if_epochs_are_different(mock_cal, mock_images):
    throws_inhomogeneous_set_exception(BiasComparer, None, 'epoch', '20160102')


@mock.patch('banzai.stages.Image')
@mock.patch('banzai.stages.ApplyCalibration.get_calibration_filename')
def test_raises_an_exection_if_nx_are_different(mock_cal, mock_images):
    mock_cal.return_value = 'test.fits'
    throws_inhomogeneous_set_exception(BiasComparer, None, 'nx', 105)


@mock.patch('banzai.stages.Image')
@mock.patch('banzai.stages.ApplyCalibration.get_calibration_filename')
def test_raises_an_exection_if_ny_are_different(mock_cal, mock_images):
    mock_cal.return_value = 'test.fits'
    throws_inhomogeneous_set_exception(BiasComparer, None, 'ny', 107)


@mock.patch('banzai.stages.Image')
@mock.patch('banzai.stages.ApplyCalibration.get_calibration_filename')
@mock.patch('banzai.stages.Stage.save_qc_results')
def test_does_not_raise_exception_if_no_master_calibration(mock_save_qc, mock_cal, mock_images):
    mock_cal.return_value = None
    mock_images.return_value = FakeImage()
    comparer = BiasComparer(None)
    images = comparer.do_stage([FakeImage() for x in range(6)])
    assert len(images) == 6


@mock.patch('banzai.stages.Image')
@mock.patch('banzai.stages.ApplyCalibration.get_calibration_filename')
@mock.patch('banzai.stages.Stage.save_qc_results')
def test_does_not_reject_noisy_images(mock_save_qc, mock_cal, mock_image):
    mock_cal.return_value = 'test.fits'
    master_readnoise = 3.0
    nx = 101
    ny = 103

    fake_master_bias = FakeImage()
    fake_master_bias.data = np.random.normal(0.0, master_readnoise, size=(ny, nx))
    mock_image.return_value = fake_master_bias

    comparer = BiasComparer(None)
    images = [FakeImage(image_multiplier=0.0) for x in range(6)]
    for image in images:
        image.data = np.random.normal(0.0, image.readnoise, size=(ny, nx))

    images = comparer.do_stage(images)

    assert len(images) == 6


@mock.patch('banzai.stages.Image')
@mock.patch('banzai.stages.ApplyCalibration.get_calibration_filename')
@mock.patch('banzai.stages.Stage.save_qc_results')
def test_does_reject_bad_images(mock_save_qc, mock_cal, mock_image):
    mock_cal.return_value = 'test.fits'
    master_readnoise = 3.0
    nx = 101
    ny = 103

    fake_master_bias = FakeImage()
    fake_master_bias.data = np.random.normal(0.0, master_readnoise, size=(ny, nx))
    mock_image.return_value = fake_master_bias

    comparer = BiasComparer(None)
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
