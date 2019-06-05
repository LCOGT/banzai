import numpy as np

from banzai.utils import stats
from banzai.dark import DarkMaker
from banzai.tests.utils import FakeContext, handles_inhomogeneous_set
from banzai.tests.dark_utils import FakeDarkImage
import mock



def test_min_images():
    dark_maker = DarkMaker(FakeContext())
    processed_images = dark_maker.do_stage([])
    assert len(processed_images) == 0


def test_group_by_attributes():
    maker = DarkMaker(FakeContext())
    assert maker.group_by_attributes() == ['configuration_mode', 'ccdsum']


@mock.patch('banzai.calibrations.FRAME_CLASS', side_effect=FakeDarkImage)
def test_header_cal_type_dark(mock_frame):
    context = FakeContext()
    context.FRAME_CLASS = FakeDarkImage

    maker = DarkMaker(context)

    images = maker.do_stage([FakeDarkImage() for x in range(6)])
    assert images[0].header['OBSTYPE'].upper() == 'DARK'


def test_returns_null_if_configuration_modes_are_different():
    handles_inhomogeneous_set(DarkMaker, FakeContext(),'configuration_mode', 'central_2k_2x2', calibration_maker=True)


def test_returns_null_if_nx_are_different():
    handles_inhomogeneous_set(DarkMaker, FakeContext(), 'nx', 105, calibration_maker=True)


def test_returns_null_if_ny_are_different():
    handles_inhomogeneous_set(DarkMaker, FakeContext(), 'ny', 107, calibration_maker=True)


@mock.patch('banzai.calibrations.FRAME_CLASS', side_effect=FakeDarkImage)
def test_makes_a_sensible_master_dark(mock_frame):
    nimages = 20
    images = [FakeDarkImage() for x in range(nimages)]
    for i, image in enumerate(images):
        image.data = np.ones((image.ny, image.nx)) * i

    expected_master_dark = stats.sigma_clipped_mean(np.arange(nimages), 3.0)

    maker = DarkMaker(FakeContext(frame_class=FakeDarkImage))
    stacked_images = maker.do_stage(images)
    assert (stacked_images[0].data == expected_master_dark).all()
