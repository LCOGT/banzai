import numpy as np

from banzai.utils import stats
from banzai.dark import DarkMaker
from banzai.tests.utils import FakeContext, throws_inhomogeneous_set_exception
from banzai.tests.dark_utils import FakeDarkImage


def test_min_images():
    dark_maker = DarkMaker(FakeContext())
    processed_images = dark_maker.do_stage([])
    assert len(processed_images) == 0


def test_group_by_attributes():
    maker = DarkMaker(FakeContext())
    assert maker.group_by_attributes == ['ccdsum']


def test_header_cal_type_dark():
    context = FakeContext()
    context.FRAME_CLASS = FakeDarkImage

    maker = DarkMaker(context)

    images = maker.do_stage([FakeDarkImage() for x in range(6)])
    assert images[0].header['OBSTYPE'].upper() == 'DARK'


def test_raises_an_exception_if_ccdsums_are_different():
    throws_inhomogeneous_set_exception(DarkMaker, FakeContext(), 'ccdsum', '1 1')


def test_raises_an_exception_if_epochs_are_different():
    throws_inhomogeneous_set_exception(DarkMaker, FakeContext(), 'epoch', '20160102')


def test_raises_an_exception_if_nx_are_different():
    throws_inhomogeneous_set_exception(DarkMaker, FakeContext(), 'nx', 105)


def test_raises_an_exception_if_ny_are_different():
    throws_inhomogeneous_set_exception(DarkMaker, FakeContext(), 'ny', 107)


def test_makes_a_sensible_master_dark():
    nimages = 20
    images = [FakeDarkImage() for x in range(nimages)]
    for i, image in enumerate(images):
        image.data = np.ones((image.ny, image.nx)) * i

    expected_master_dark = stats.sigma_clipped_mean(np.arange(nimages), 3.0)

    maker = DarkMaker(FakeContext(frame_class=FakeDarkImage))
    stacked_images = maker.do_stage(images)
    assert (stacked_images[0].data == expected_master_dark).all()
