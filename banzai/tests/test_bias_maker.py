from __future__ import absolute_import, division, print_function, unicode_literals
from banzai.bias import BiasMaker
import numpy as np

from banzai.tests.utils import FakeImage, FakeContext, throws_inhomogeneous_set_exception

import mock


class FakeBiasImage(FakeImage):
    def __init__(self, *args, **kwargs):
        super(FakeBiasImage, self).__init__(*args, **kwargs)
        self.caltype = 'bias'
        self.header = {'OBSTYPE': 'BIAS'}


def test_min_images():

    bias_maker = BiasMaker(None)
    processed_images = bias_maker.do_stage([])
    assert len(processed_images) == 0


def test_group_by_keywords():
    maker = BiasMaker(None)
    assert maker.group_by_keywords == ['ccdsum']


@mock.patch('banzai.bias.Image')
def test_header_master_bias_level_returns_1(mock_image):
    maker = BiasMaker(FakeContext())

    maker.do_stage([FakeBiasImage() for x in range(6)])

    args, kwargs = mock_image.call_args
    header = kwargs['header']
    assert header['BIASLVL'] == 1.0


@mock.patch('banzai.bias.Image')
def test_header_master_bias_level_returns_2(mock_image):
    maker = BiasMaker(FakeContext())

    maker.do_stage([FakeBiasImage(image_multiplier=2.0) for x in range(6)])

    args, kwargs = mock_image.call_args
    header = kwargs['header']
    assert header['BIASLVL'] == 2.0


@mock.patch('banzai.bias.Image')
def test_header_cal_type_bias(mock_image):

    maker = BiasMaker(FakeContext())

    maker.do_stage([FakeBiasImage() for x in range(6)])

    args, kwargs = mock_image.call_args
    header = kwargs['header']
    assert header['OBSTYPE'].upper() == 'BIAS'


@mock.patch('banzai.bias.Image')
def test_raises_an_exection_if_ccdsums_are_different(mock_images):
    throws_inhomogeneous_set_exception(BiasMaker, FakeContext(), 'ccdsum', '1 1')


@mock.patch('banzai.bias.Image')
def test_raises_an_exection_if_epochs_are_different(mock_images):
    throws_inhomogeneous_set_exception(BiasMaker, FakeContext(), 'epoch', '20160102')


@mock.patch('banzai.bias.Image')
def test_raises_an_exection_if_nx_are_different(mock_images):
    throws_inhomogeneous_set_exception(BiasMaker, FakeContext(), 'nx', 105)


@mock.patch('banzai.bias.Image')
def test_raises_an_exection_if_ny_are_different(mock_images):
    throws_inhomogeneous_set_exception(BiasMaker, FakeContext(), 'ny', 107)


@mock.patch('banzai.bias.Image')
def test_makes_a_sensible_master_bias(mock_images):
    nimages = 20
    expected_bias = 1183.0
    expected_readnoise = 15.0

    images = [FakeBiasImage() for x in range(nimages)]
    for image in images:
        image.data = np.random.normal(loc=expected_bias, scale=expected_readnoise,
                                      size=(image.ny, image.nx))

    maker = BiasMaker(FakeContext())
    maker.do_stage(images)

    args, kwargs = mock_images.call_args
    master_bias = kwargs['data']
    assert np.abs(np.mean(master_bias)) < 0.1
    actual_bias = float(kwargs['header']['BIASLVL'])
    assert np.abs(actual_bias - expected_bias) < 0.1
    actual_readnoise = np.std(master_bias)
    assert np.abs(actual_readnoise - expected_readnoise / (nimages ** 0.5)) < 0.2
