from ..bias import BiasMaker, InhomogeneousSetException
import numpy as np
from datetime import datetime
from ..main import Image

import mock
import pytest


class FakeImage(Image):
    def __init__(self, nx=101, ny=103, image_multiplier=1.0, ccdsum='2 2',
                 epoch='2016-01-01'):
        self.nx = nx
        self.ny = ny
        self.data = image_multiplier * np.ones((self.ny, self.nx))
        self.filename = 'test.fits'
        self.ccdsum = ccdsum
        self.epoch = epoch
        self.instrument = 'get_calibration_filename'
        self.header = {'OBSTYPE': 'BIAS', 'CCDSUM': '2 2'}
        self.filter = 'U'
        self.telescope_id = -1
        self.dateobs = datetime(2016, 1, 1)

    def get_calibration_filename(self):
        return '/tmp/bias_{0}_{1}_bin{2}.fits'.format(self.instrument, self.epoch,
                                                      self.ccdsum.replace(' ', 'x'))


class FakeContext(object):
    def __init__(self):
        self.processed_path = '/tmp'


def test_min_images():

    bias_maker = BiasMaker(None)
    processed_images = bias_maker.do_stage([])
    assert len(processed_images) == 0


def test_group_by_keywords():
    maker = BiasMaker(None)
    assert maker.group_by_keywords == ['ccdsum']


@mock.patch('pylcogt.bias.Image')
def test_header_master_bias_level_returns_1(mock_image):
    maker = BiasMaker(FakeContext())

    maker.do_stage([FakeImage() for x in range(6)])

    args, kwargs = mock_image.call_args
    header = kwargs['header']
    assert header['BIASLVL'] == 1.0


@mock.patch('pylcogt.bias.Image')
def test_header_master_bias_level_returns_2(mock_image):
    maker = BiasMaker(FakeContext())

    maker.do_stage([FakeImage(image_multiplier=2.0) for x in range(6)])

    args, kwargs = mock_image.call_args
    header = kwargs['header']
    assert header['BIASLVL'] == 2.0


@mock.patch('pylcogt.bias.Image')
def test_header_cal_type_bias(mock_image):

    maker = BiasMaker(FakeContext())

    maker.do_stage([FakeImage() for x in range(6)])

    args, kwargs = mock_image.call_args
    header = kwargs['header']
    assert header['OBSTYPE'].upper() == 'BIAS'


@mock.patch('pylcogt.bias.Image')
def test_ccdsum_header(mock_image):
    maker = BiasMaker(FakeContext())

    maker.do_stage([FakeImage() for x in range(6)])

    args, kwargs = mock_image.call_args
    header = kwargs['header']
    assert header['CCDSUM'] == '2 2'


def test_raises_an_exection_if_ccdsums_are_different():
    maker = BiasMaker(FakeContext())

    with pytest.raises(InhomogeneousSetException) as exception_info:
        maker.do_stage([FakeImage(ccdsum='1 1')] + [FakeImage() for x in range(6)])
    assert 'Images have different ccdsums' == str(exception_info.value)


def test_raises_an_exection_if_epochs_are_different():
    maker = BiasMaker(FakeContext())

    with pytest.raises(InhomogeneousSetException) as exception_info:
        maker.do_stage([FakeImage(epoch='2016-01-02')] + [FakeImage() for x in range(6)])
    assert 'Images have different epochs' == str(exception_info.value)


def test_raises_an_exection_if_nx_are_different():
    maker = BiasMaker(FakeContext())

    with pytest.raises(InhomogeneousSetException) as exception_info:
        maker.do_stage([FakeImage(nx=105)] + [FakeImage() for x in range(6)])
    assert 'Images have different nxs' == str(exception_info.value)


def test_raises_an_exection_if_ny_are_different():
    maker = BiasMaker(FakeContext())

    with pytest.raises(InhomogeneousSetException) as exception_info:
        maker.do_stage([FakeImage(ny=107)] + [FakeImage() for x in range(6)])
    assert 'Images have different nys' == str(exception_info.value)


@mock.patch('pylcogt.bias.Image')
def test_makes_a_sensible_master_bias(mock_images):
    nimages = 20
    expected_bias = 1183.0
    expected_readnoise = 15.0

    images = [FakeImage() for x in range(20)]
    for image in images:
        image.data = np.random.normal(loc=expected_bias, scale=expected_readnoise,
                                      size=(image.ny, image.nx))

    maker = BiasMaker(FakeContext())
    maker.do_stage(images)

    args, kwargs = mock_images.call_args
    master_bias = kwargs['data']
    np.testing.assert_allclose(np.mean(master_bias), 0.0, atol=0.1)
    actual_bias = float(kwargs['header']['BIASLVL'])
    assert np.abs(actual_bias - expected_bias) < 0.1
    actual_readnoise = np.std(master_bias)
    assert np.abs(actual_readnoise - expected_readnoise / (nimages ** 0.5)) < 0.2
