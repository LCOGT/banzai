import mock
import numpy as np

from banzai.flats import FlatMaker
from banzai.tests.utils import FakeContext, throws_inhomogeneous_set_exception
from banzai.tests.flat_utils import FakeFlatImage


def test_min_images():
    flat_maker = FlatMaker(FakeContext())
    processed_images = flat_maker.do_stage([])
    assert len(processed_images) == 0


def test_group_by_attributes():
    maker = FlatMaker(FakeContext())
    assert maker.group_by_attributes() == ['ccdsum', 'filter']


@mock.patch('banzai.images.Image._init_instrument_info')
def test_header_cal_type_flat(mock_instrument_info):

    mock_instrument_info.return_value = None, None, None
    fake_context = FakeContext()
    fake_context.db_address = ''

    maker = FlatMaker(fake_context)
    master_flat = maker.do_stage([FakeFlatImage() for x in range(6)])[0]

    header = master_flat.header
    assert header['OBSTYPE'].upper() == 'SKYFLAT'


def test_raises_an_exception_if_ccdsums_are_different(caplog):
    throws_inhomogeneous_set_exception(caplog, FlatMaker, FakeContext(), 'ccdsum', '1 1', calibration_maker=True)


def test_raises_an_exception_if_nx_are_different(caplog):
    throws_inhomogeneous_set_exception(caplog, FlatMaker, FakeContext(), 'nx', 105, calibration_maker=True)


def test_raises_an_exception_if_ny_are_different(caplog):
    throws_inhomogeneous_set_exception(caplog, FlatMaker, FakeContext(), 'ny', 107, calibration_maker=True)


def test_raises_an_exception_if_filters_are_different(caplog):
    throws_inhomogeneous_set_exception(caplog, FlatMaker, FakeContext(), 'filter', 'w', calibration_maker=True)


def test_makes_a_sensible_master_flat():
    nimages = 50
    flat_level = 10000.0
    nx = 101
    ny = 103
    images = [FakeFlatImage(flat_level, nx=nx, ny=ny) for _ in range(nimages)]
    flat_pattern = np.random.normal(1.0, 0.05, size=(ny, nx))
    for i, image in enumerate(images):
        image.data = flat_pattern + np.random.normal(0.0, 0.02, size=(ny, nx))

    maker = FlatMaker(FakeContext(frame_class=FakeFlatImage))
    stacked_images = maker.do_stage(images)
    np.testing.assert_allclose(stacked_images[0].data, flat_pattern, atol=0.01, rtol=0.01)
