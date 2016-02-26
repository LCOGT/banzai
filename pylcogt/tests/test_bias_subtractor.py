from ..bias import BiasSubtractor
from .utils import FakeImage, throws_inhomogeneous_set_exception
import mock
import pytest

import numpy as np


class FakeBiasImage(FakeImage):
    def __init__(self, *args, bias_level=0.0, **kwargs):
        super(FakeBiasImage, self).__init__(*args, image_multiplier=bias_level, **kwargs)
        self.header = {'BIASLVL': bias_level}


def test_no_input_images():
    subtractor = BiasSubtractor(None)
    images = subtractor.do_stage([])
    assert len(images) == 0


def test_group_by_keywords():
    subtractor = BiasSubtractor(None)
    assert subtractor.group_by_keywords == ['ccdsum']


@mock.patch('pylcogt.bias.Image')
@mock.patch('pylcogt.bias.BiasSubtractor.get_calibration_filename')
def test_header_has_biaslevel(mock_cal, mock_image):
    mock_image.return_value = FakeBiasImage()
    subtractor = BiasSubtractor(None)
    images = subtractor.do_stage([FakeBiasImage() for x in range(6)])
    for image in images:
        assert image.header['BIASLVL'] == 0


@mock.patch('pylcogt.bias.Image')
@mock.patch('pylcogt.bias.BiasSubtractor.get_calibration_filename')
def test_header_biaslevel_is_1(mock_cal, mock_image):
    mock_image.return_value = FakeBiasImage(bias_level=1)
    subtractor = BiasSubtractor(None)
    images = subtractor.do_stage([FakeBiasImage() for x in range(6)])
    for image in images:
        assert image.header['BIASLVL'] == 1


@mock.patch('pylcogt.bias.Image')
@mock.patch('pylcogt.bias.BiasSubtractor.get_calibration_filename')
def test_header_biaslevel_is_2(mock_cal, mock_image):
    mock_image.return_value = FakeBiasImage(bias_level=2.0)
    subtractor = BiasSubtractor(None)
    images = subtractor.do_stage([FakeBiasImage() for x in range(6)])
    for image in images:
        assert image.header['BIASLVL'] == 2


@mock.patch('pylcogt.bias.Image')
def test_raises_an_exection_if_ccdsums_are_different(mock_images):
    throws_inhomogeneous_set_exception(BiasSubtractor, None, 'ccdsum', '1 1')


@mock.patch('pylcogt.bias.Image')
def test_raises_an_exection_if_epochs_are_different(mock_images):
    throws_inhomogeneous_set_exception(BiasSubtractor, None, 'epoch', '20160102')


@mock.patch('pylcogt.bias.Image')
def test_raises_an_exection_if_nx_are_different(mock_images):
    throws_inhomogeneous_set_exception(BiasSubtractor, None, 'nx', 105)


@mock.patch('pylcogt.bias.Image')
def test_raises_an_exection_if_ny_are_different(mock_images):
    throws_inhomogeneous_set_exception(BiasSubtractor, None, 'ny', 107)
