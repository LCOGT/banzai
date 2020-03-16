import mock
import numpy as np
import pytest
from astropy.io.fits import Header

from banzai.flats import FlatMaker
from banzai.tests.utils import FakeContext, FakeLCOObservationFrame, FakeCCDData

pytestmark = pytest.mark.flat_maker


def test_min_images():
    flat_maker = FlatMaker(FakeContext())
    processed_image = flat_maker.do_stage([])
    assert processed_image is None


def test_group_by_attributes():
    maker = FlatMaker(FakeContext())
    assert maker.group_by_attributes() == ['configuration_mode', 'binning', 'filter']


@mock.patch('banzai.utils.file_utils.make_calibration_filename_function')
def test_header_cal_type_flat(mock_namer):
    mock_namer.return_value = lambda *x: 'foo.fits'

    maker = FlatMaker(FakeContext())
    image_header = Header({'DATE-OBS': '2019-12-04T14:34:00',
                           'DETSEC': '[1:100,1:100]',
                           'DATASEC': '[1:100,1:100]',
                           'OBSTYPE': 'SKYFLAT'})
    master_flat = maker.do_stage([FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta=image_header)])
                                  for x in range(6)])

    assert master_flat.meta['OBSTYPE'].upper() == 'SKYFLAT'


@mock.patch('banzai.utils.file_utils.make_calibration_filename_function')
def test_makes_a_sensible_master_flat(mock_namer):
    mock_namer.return_value = lambda *x: 'foo.fits'
    nimages = 50
    flat_level = 10000.0
    nx = 100
    ny = 100
    master_flat_variation = 0.05

    image_header = Header({'DATE-OBS': '2019-12-04T14:34:00',
                           'DETSEC': '[1:100,1:100]',
                           'DATASEC': '[1:100,1:100]',
                           'FLATLVL': flat_level,
                           'OBSTYPE': 'SKYFLAT'})

    flat_pattern = np.random.normal(1.0, master_flat_variation, size=(ny, nx))
    images = [FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=flat_pattern + np.random.normal(0.0, 0.02, size=(ny, nx)),
                                                            meta=image_header)])
              for _ in range(nimages)]

    maker = FlatMaker(FakeContext())
    stacked_image = maker.do_stage(images)
    np.testing.assert_allclose(stacked_image.primary_hdu.data, flat_pattern, atol=0.1, rtol=0.1)
