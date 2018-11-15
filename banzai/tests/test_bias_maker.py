import mock
import numpy as np
from astropy.io import fits

from banzai.bias import BiasMaker
from banzai.tests.utils import FakeImage, FakeContext, throws_inhomogeneous_set_exception


class FakeBiasImage(FakeImage):
    def __init__(self, *args, bias_level=0.0, **kwargs):
        super(FakeBiasImage, self).__init__(*args, **kwargs)
        self.caltype = 'bias'
        self.header = fits.Header()
        self.header['OBSTYPE'] = 'BIAS'
        self.header['BIASLVL'] = bias_level


def test_min_images():
    bias_maker = BiasMaker(None)
    processed_images = bias_maker.do_stage([])
    assert len(processed_images) == 0


def test_group_by_attributes():
    maker = BiasMaker(None)
    assert maker.group_by_attributes == ['ccdsum']


@mock.patch('banzai.calibrations.Image')
def test_header_cal_type_bias(mock_image):

    maker = BiasMaker(FakeContext())

    maker.do_stage([FakeBiasImage() for x in range(6)])

    args, kwargs = mock_image.call_args
    header = kwargs['header']
    assert header['OBSTYPE'].upper() == 'BIAS'


@mock.patch('banzai.calibrations.Image')
def test_raises_an_exception_if_ccdsums_are_different(mock_images):
    throws_inhomogeneous_set_exception(BiasMaker, FakeContext(), 'ccdsum', '1 1', calibration_maker=True)


@mock.patch('banzai.calibrations.Image')
def test_raises_an_exception_if_epochs_are_different(mock_images):
    throws_inhomogeneous_set_exception(BiasMaker, FakeContext(), 'epoch', '20160102', calibration_maker=True)


@mock.patch('banzai.calibrations.Image')
def test_raises_an_exception_if_nx_are_different(mock_images):
    throws_inhomogeneous_set_exception(BiasMaker, FakeContext(), 'nx', 105, calibration_maker=True)


@mock.patch('banzai.calibrations.Image')
def test_raises_an_exception_if_ny_are_different(mock_images):
    throws_inhomogeneous_set_exception(BiasMaker, FakeContext(), 'ny', 107, calibration_maker=True)


@mock.patch('banzai.calibrations.Image._init_telescope_info')
def test_bias_level_is_average_of_inputs(mock_telescope_info):
    nimages = 20
    bias_levels = np.arange(nimages, dtype=float)

    images = [FakeBiasImage(bias_level=i) for i in bias_levels]

    mock_telescope_info.return_value = None, None, None
    fake_context = FakeContext()
    fake_context.db_address = ''

    maker = BiasMaker(fake_context)
    master_bias = maker.do_stage(images)[0]

    header = master_bias.header

    assert header['BIASLVL'] == np.mean(bias_levels)


@mock.patch('banzai.calibrations.Image')
def test_makes_a_sensible_master_bias(mock_images):
    nimages = 20
    expected_readnoise = 15.0

    images = [FakeBiasImage() for x in range(nimages)]
    for image in images:
        image.data = np.random.normal(loc=0.0, scale=expected_readnoise,
                                      size=(image.ny, image.nx))

    maker = BiasMaker(FakeContext())
    maker.do_stage(images)

    args, kwargs = mock_images.call_args
    master_bias = kwargs['data']

    assert np.abs(np.mean(master_bias)) < 0.1
    actual_readnoise = np.std(master_bias)
    assert np.abs(actual_readnoise - expected_readnoise / (nimages ** 0.5)) < 0.2
