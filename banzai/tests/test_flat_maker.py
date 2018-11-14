import mock
from astropy.io import fits

from banzai.flats import FlatMaker
from banzai.tests.utils import FakeImage, FakeContext, throws_inhomogeneous_set_exception


class FakeFlatImage(FakeImage):
    def __init__(self, *args, **kwargs):
        super(FakeFlatImage, self).__init__(*args, **kwargs)
        self.caltype = 'skyflat'
        self.header = fits.Header()
        self.header['OBSTYPE'] = 'SKYFLAT'


def test_min_images():
    flat_maker = FlatMaker(None)
    processed_images = flat_maker.do_stage([])
    assert len(processed_images) == 0


def test_group_by_attributes():
    maker = FlatMaker(None)
    assert maker.group_by_attributes == ['ccdsum', 'filter']


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.flats.FlatMaker.final_step')
def test_header_cal_type_flat(mock_final_step, mock_image):

    maker = FlatMaker(FakeContext())

    maker.do_stage([FakeFlatImage() for x in range(6)])

    args, kwargs = mock_image.call_args
    header = kwargs['header']
    assert header['OBSTYPE'].upper() == 'SKYFLAT'


@mock.patch('banzai.calibrations.Image')
def test_raises_an_exception_if_ccdsums_are_different(mock_images):
    throws_inhomogeneous_set_exception(FlatMaker, FakeContext(), 'ccdsum', '1 1')


@mock.patch('banzai.calibrations.Image')
def test_raises_an_exception_if_epochs_are_different(mock_images):
    throws_inhomogeneous_set_exception(FlatMaker, FakeContext(), 'epoch', '20160102')


@mock.patch('banzai.calibrations.Image')
def test_raises_an_exception_if_nx_are_different(mock_images):
    throws_inhomogeneous_set_exception(FlatMaker, FakeContext(), 'nx', 105)


@mock.patch('banzai.calibrations.Image')
def test_raises_an_exception_if_ny_are_different(mock_images):
    throws_inhomogeneous_set_exception(FlatMaker, FakeContext(), 'ny', 107)


@mock.patch('banzai.calibrations.Image')
def test_raises_an_exception_if_filters_are_different(mock_images):
    throws_inhomogeneous_set_exception(FlatMaker, FakeContext(), 'filter', 'w')
