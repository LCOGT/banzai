import mock
import numpy as np

from banzai.bias import BiasMaker
from banzai.tests.utils import FakeContext, throws_inhomogeneous_set_exception
from banzai.tests.bias_utils import FakeBiasImage, make_context_with_master_bias


def test_min_images():
    bias_maker = BiasMaker(FakeContext())
    processed_images = bias_maker.do_stage([])
    assert len(processed_images) == 0


def test_group_by_attributes():
    maker = BiasMaker(FakeContext())
    assert maker.group_by_attributes() == ['ccdsum']


def test_header_cal_type_bias():
    nx = 101
    ny = 103
    context = make_context_with_master_bias(bias_level=0.0, readnoise=10.0, nx=nx, ny=ny)
    maker = BiasMaker(context)

    images = maker.do_stage([FakeBiasImage(nx=nx, ny=ny) for x in range(6)])
    assert images[0].header['OBSTYPE'].upper() == 'BIAS'


def test_raises_an_exception_if_ccdsums_are_different(caplog):
    throws_inhomogeneous_set_exception(caplog, BiasMaker, FakeContext(), 'ccdsum', '1 1', calibration_maker=True)


def test_raises_an_exception_if_nx_are_different(caplog):
    throws_inhomogeneous_set_exception(caplog, BiasMaker, FakeContext(), 'nx', 105, calibration_maker=True)


def test_raises_an_exception_if_ny_are_different(caplog):
    throws_inhomogeneous_set_exception(caplog, BiasMaker, FakeContext(), 'ny', 107, calibration_maker=True)


@mock.patch('banzai.images.Image._init_instrument_info')
def test_bias_level_is_average_of_inputs(mock_instrument_info):
    nimages = 20
    bias_levels = np.arange(nimages, dtype=float)

    images = [FakeBiasImage(bias_level=i) for i in bias_levels]

    mock_instrument_info.return_value = None, None, None
    fake_context = FakeContext()
    fake_context.db_address = ''

    maker = BiasMaker(fake_context)
    master_bias = maker.do_stage(images)[0]

    header = master_bias.header

    assert header['BIASLVL'] == np.mean(bias_levels)


def test_makes_a_sensible_master_bias():
    nimages = 20
    expected_readnoise = 15.0

    images = [FakeBiasImage() for x in range(nimages)]
    for image in images:
        image.data = np.random.normal(loc=0.0, scale=expected_readnoise,
                                      size=(image.ny, image.nx))

    maker = BiasMaker(FakeContext(frame_class=FakeBiasImage))
    stacked_images = maker.do_stage(images)
    master_bias = stacked_images[0].data
    assert np.abs(np.mean(master_bias)) < 0.1
    actual_readnoise = np.std(master_bias)
    assert np.abs(actual_readnoise - expected_readnoise / (nimages ** 0.5)) < 0.2
