from ..bias import BiasMaker, InhomogenousSetException
import numpy as np
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
        self.header = {'OBSTYPE': 'bias'}
        self.filter = 'U'
        self.telescope_id = -1

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


# Test completion
@mock.patch('pylcogt.bias.dbs')
def test_foo(mock_dbs):
    maker = BiasMaker(FakeContext())
    maker.do_stage([FakeImage() for x in range(6)])


@mock.patch('pylcogt.bias.fits')
@mock.patch('pylcogt.bias.dbs')
def test_header_master_bias_level_returns_1(mock_dbs, mock_fits):

    header = mock_fits.Header()
    maker = BiasMaker(FakeContext())

    maker.do_stage([FakeImage() for x in range(6)])

    assert mock.call('BIASLVL', 1.0) in header.__setitem__.call_args_list


# Triangulation
@mock.patch('pylcogt.bias.fits')
@mock.patch('pylcogt.bias.dbs')
def test_header_master_bias_level_returns_2(mock_dbs, mock_fits):

    header = mock_fits.Header()
    maker = BiasMaker(FakeContext())

    maker.do_stage([FakeImage(image_multiplier=2.0) for x in range(6)])

    assert mock.call('BIASLVL', 2.0) in header.__setitem__.call_args_list

@mock.patch('pylcogt.bias.fits')
@mock.patch('pylcogt.bias.dbs')
def test_header_cal_type_bias(mock_dbs, mock_fits):

    header = mock_fits.Header()
    maker = BiasMaker(FakeContext())

    maker.do_stage([FakeImage() for x in range(6)])

    assert mock.call('CALTYPE', 'BIAS') in header.__setitem__.call_args_list

# def test_nan_in_frame

# def test_negative_in frame


@mock.patch('pylcogt.bias.fits')
@mock.patch('pylcogt.bias.dbs')
def test_ccdsum_header(mock_dbs, mock_fits):
    header = mock_fits.Header()
    maker = BiasMaker(FakeContext())

    maker.do_stage([FakeImage() for x in range(6)])

    assert mock.call('CCDSUM', '2 2') in header.__setitem__.call_args_list


@mock.patch('pylcogt.bias.fits')
@mock.patch('pylcogt.bias.dbs')
def test_raises_an_exection_if_ccdsums_are_different(mock_dbs, mock_fits):
    header = mock_fits.Header()
    maker = BiasMaker(FakeContext())

    with pytest.raises(InhomogenousSetException) as exception_info:
        maker.do_stage([FakeImage(ccdsum='1 1')] + [FakeImage() for x in range(6)])
    assert 'Images have different ccdsums' == str(exception_info.value)


@mock.patch('pylcogt.bias.fits')
@mock.patch('pylcogt.bias.dbs')
def test_raises_an_exection_if_epochs_are_different(mock_dbs, mock_fits):
    header = mock_fits.Header()
    maker = BiasMaker(FakeContext())

    with pytest.raises(InhomogenousSetException) as exception_info:
        maker.do_stage([FakeImage(epoch='2016-01-02')] + [FakeImage() for x in range(6)])
    assert 'Images have different epochs' == str(exception_info.value)


@mock.patch('pylcogt.bias.fits')
@mock.patch('pylcogt.bias.dbs')
def test_raises_an_exection_if_nx_are_different(mock_dbs, mock_fits):
    header = mock_fits.Header()
    maker = BiasMaker(FakeContext())

    with pytest.raises(InhomogenousSetException) as exception_info:
        maker.do_stage([FakeImage(nx=105)] + [FakeImage() for x in range(6)])
    assert 'Images have different nxs' == str(exception_info.value)


@mock.patch('pylcogt.bias.fits')
@mock.patch('pylcogt.bias.dbs')
def test_raises_an_exection_if_ny_are_different(mock_dbs, mock_fits):
    header = mock_fits.Header()
    maker = BiasMaker(FakeContext())

    with pytest.raises(InhomogenousSetException) as exception_info:
        maker.do_stage([FakeImage(ny=107)] + [FakeImage() for x in range(6)])
    assert 'Images have different nys' == str(exception_info.value)


@mock.patch('pylcogt.bias.fits')
@mock.patch('pylcogt.bias.dbs')
def test_write_fits_file(mock_dbs, mock_fits):
    header = mock_fits.Header()
    maker = BiasMaker(FakeContext())

    maker.do_stage([FakeImage() for x in range(6)])
    image = FakeImage()
    args, kwargs = mock_fits.writeto.call_args
    assert args[0] == image.get_calibration_filename()
    assert (args[1] == np.zeros((image.ny, image.nx))).sum() == (image.nx * image.ny)
    assert kwargs['header'] == header
    assert kwargs['clobber'] == True
