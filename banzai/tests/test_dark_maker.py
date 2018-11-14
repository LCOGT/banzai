import mock
import numpy as np
from astropy.io import fits

from banzai.utils import stats
from banzai.dark import DarkMaker
from banzai.tests.utils import FakeImage, FakeContext, throws_inhomogeneous_set_exception


class FakeDarkImage(FakeImage):
    def __init__(self, *args, **kwargs):
        super(FakeDarkImage, self).__init__(*args, **kwargs)
        self.caltype = 'dark'
        self.header = fits.Header()
        self.header['OBSTYPE'] = 'DARK'


def test_min_images():
    dark_maker = DarkMaker(None)
    processed_images = dark_maker.do_stage([])
    assert len(processed_images) == 0


def test_group_by_attributes():
    maker = DarkMaker(None)
    assert maker.group_by_attributes == ['ccdsum']


@mock.patch('banzai.calibrations.Image')
def test_header_cal_type_dark(mock_image):

    maker = DarkMaker(FakeContext())

    maker.do_stage([FakeDarkImage() for x in range(6)])

    args, kwargs = mock_image.call_args
    header = kwargs['header']
    assert header['OBSTYPE'].upper() == 'DARK'


@mock.patch('banzai.calibrations.Image')
def test_raises_an_exception_if_ccdsums_are_different(mock_images):
    throws_inhomogeneous_set_exception(DarkMaker, FakeContext(), 'ccdsum', '1 1')


@mock.patch('banzai.calibrations.Image')
def test_raises_an_exception_if_epochs_are_different(mock_images):
    throws_inhomogeneous_set_exception(DarkMaker, FakeContext(), 'epoch', '20160102')


@mock.patch('banzai.calibrations.Image')
def test_raises_an_exception_if_nx_are_different(mock_images):
    throws_inhomogeneous_set_exception(DarkMaker, FakeContext(), 'nx', 105)


@mock.patch('banzai.calibrations.Image')
def test_raises_an_exception_if_ny_are_different(mock_images):
    throws_inhomogeneous_set_exception(DarkMaker, FakeContext(), 'ny', 107)


@mock.patch('banzai.calibrations.Image')
def test_makes_a_sensible_master_dark(mock_images):
    nimages = 20
    images = [FakeDarkImage() for x in range(nimages)]
    for i, image in enumerate(images):
        image.data = np.ones((image.ny, image.nx)) * i

    expected_master_dark = stats.sigma_clipped_mean(np.arange(nimages), 3.0)

    maker = DarkMaker(FakeContext())
    maker.do_stage(images)

    args, kwargs = mock_images.call_args
    master_dark = kwargs['data']

    assert (master_dark == expected_master_dark).all()
