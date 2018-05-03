from __future__ import absolute_import, division, print_function, unicode_literals
from banzai.bias import BiasMasterLevelSubtractor
from banzai.tests.utils import FakeImage, throws_inhomogeneous_set_exception
from banzai.stages import MasterCalibrationDoesNotExist
import pytest
import mock

import numpy as np


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(9723492)


class FakeBiasImage(FakeImage):
    def __init__(self, bias_level=0.0):
        super(FakeBiasImage, self).__init__(image_multiplier=bias_level)
        self.header = {'BIASLVL': bias_level}


def test_no_input_images():
    subtractor = BiasMasterLevelSubtractor(None)
    images = subtractor.do_stage([])
    assert len(images) == 0


def test_group_by_keywords():
    subtractor = BiasMasterLevelSubtractor(None)
    assert subtractor.group_by_keywords == ['ccdsum']


@mock.patch('banzai.stages.Image')
@mock.patch('banzai.stages.ApplyCalibration.get_calibration_filename')
def test_header_has_mbiaslevel(mock_cal, mock_image):
    mock_image.return_value = FakeBiasImage()
    subtractor = BiasMasterLevelSubtractor(None)
    images = subtractor.do_stage([FakeImage() for x in range(6)])
    for image in images:
        assert 'MBIASLVL' in image.header


@mock.patch('banzai.stages.Image')
@mock.patch('banzai.stages.ApplyCalibration.get_calibration_filename')
def test_header_mbiaslevel_is_1(mock_cal, mock_image):
    mock_image.return_value = FakeBiasImage(bias_level=1)
    subtractor = BiasMasterLevelSubtractor(None)
    images = subtractor.do_stage([FakeImage() for x in range(6)])
    for image in images:
        assert image.header['MBIASLVL'][0] == 1


@mock.patch('banzai.stages.Image')
@mock.patch('banzai.stages.ApplyCalibration.get_calibration_filename')
def test_header_mbiaslevel_is_2(mock_cal, mock_image):
    mock_image.return_value = FakeBiasImage(bias_level=2.0)
    subtractor = BiasMasterLevelSubtractor(None)
    images = subtractor.do_stage([FakeImage() for x in range(6)])
    for image in images:
        assert image.header['MBIASLVL'][0] == 2.0


@mock.patch('banzai.stages.Image')
@mock.patch('banzai.stages.ApplyCalibration.get_calibration_filename')
def test_raises_an_exection_if_ccdsums_are_different(mock_cal, mock_images):
    throws_inhomogeneous_set_exception(BiasMasterLevelSubtractor, None, 'ccdsum', '1 1')


@mock.patch('banzai.stages.Image')
@mock.patch('banzai.stages.ApplyCalibration.get_calibration_filename')
def test_raises_an_exection_if_epochs_are_different(mock_cal, mock_images):
    throws_inhomogeneous_set_exception(BiasMasterLevelSubtractor, None, 'epoch', '20160102')


@mock.patch('banzai.stages.Image')
@mock.patch('banzai.stages.ApplyCalibration.get_calibration_filename')
def test_raises_an_exection_if_nx_are_different(mock_cal, mock_images):
    mock_cal.return_value = 'test.fits'
    throws_inhomogeneous_set_exception(BiasMasterLevelSubtractor, None, 'nx', 105)


@mock.patch('banzai.stages.Image')
@mock.patch('banzai.stages.ApplyCalibration.get_calibration_filename')
def test_raises_an_exection_if_ny_are_different(mock_cal, mock_images):
    mock_cal.return_value = 'test.fits'
    throws_inhomogeneous_set_exception(BiasMasterLevelSubtractor, None, 'ny', 107)


@mock.patch('banzai.stages.Image')
@mock.patch('banzai.stages.ApplyCalibration.get_calibration_filename')
@mock.patch('banzai.stages.Stage.save_qc_results')
def test_does_not_raise_exception_if_no_master_calibration(mock_save_qc, mock_cal, mock_images):
    mock_cal.return_value = None
    mock_images.return_value = FakeBiasImage(30.0)

    subtractor = BiasMasterLevelSubtractor(None)
    images = subtractor.do_stage([FakeBiasImage(30.0) for x in range(6)])
    assert len(images) == 6


@mock.patch('banzai.stages.Image')
@mock.patch('banzai.stages.ApplyCalibration.get_calibration_filename')
def test_bias_master_level_subtraction_is_reasonable(mock_cal, mock_image):
    mock_cal.return_value = 'test.fits'
    input_bias = 1000.0
    input_level = 2000.0

    fake_master_bias = FakeBiasImage(bias_level=input_bias)
    mock_image.return_value = fake_master_bias

    subtractor = BiasMasterLevelSubtractor(None)
    images = [FakeImage(image_multiplier=input_level) for x in range(6)]

    images = subtractor.do_stage(images)

    for image in images:
        np.testing.assert_allclose(image.data, np.ones(image.data.shape) * (input_level - input_bias), 1e-5)
