import numpy as np
import pytest
from astropy.io.fits import Header

from banzai.utils import stats
from banzai.dark import DarkMaker
from banzai.tests.utils import FakeContext, FakeLCOObservationFrame, FakeCCDData
import mock

pytestmark = pytest.mark.dark_maker


def test_min_images():
    dark_maker = DarkMaker(FakeContext())
    processed_image = dark_maker.do_stage([])
    assert processed_image is None


def test_group_by_attributes():
    maker = DarkMaker(FakeContext())
    assert maker.group_by_attributes() == ['configuration_mode', 'binning']


@mock.patch('banzai.utils.file_utils.make_calibration_filename_function')
def test_header_cal_type_dark(mock_namer):
    mock_namer.return_value = lambda *x: 'foo.fits'
    nx = 100
    ny = 100
    maker = DarkMaker(FakeContext())

    image_header = Header({'DATE-OBS': '2019-12-04T14:34:00',
                           'DETSEC': '[1:100,1:100]',
                           'DATASEC': '[1:100,1:100]',
                           'OBSTYPE': 'DARK'})

    image = maker.do_stage([FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.zeros((ny,nx)),
                                                                           meta=image_header)])
                             for x in range(6)])

    assert image.meta['OBSTYPE'].upper() == 'DARK'


@mock.patch('banzai.utils.file_utils.make_calibration_filename_function')
def test_makes_a_sensible_master_dark(mock_namer):
    mock_namer.return_value = lambda *x: 'foo.fits'
    nimages = 20
    nx = 100
    ny = 100
    image_header = Header({'DATE-OBS': '2019-12-04T14:34:00',
                           'DETSEC': '[1:100,1:100]',
                           'DATASEC': '[1:100,1:100]',
                           'OBSTYPE': 'DARK'})
    images = [FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.ones((ny, nx)) * x,
                                                            meta=image_header)])
              for x in range(nimages)]

    expected_master_dark = stats.sigma_clipped_mean(np.arange(nimages), 3.0)
    maker = DarkMaker(FakeContext())
    stacked_image = maker.do_stage(images)

    assert (stacked_image.data == expected_master_dark).all()
