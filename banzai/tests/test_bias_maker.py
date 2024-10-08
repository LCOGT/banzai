import mock
import numpy as np
import pytest
from astropy.io.fits import Header

from banzai.bias import BiasMaker
from banzai.tests.utils import FakeContext, FakeLCOObservationFrame, FakeCCDData

pytestmark = pytest.mark.bias_maker


def test_min_images():
    bias_maker = BiasMaker(FakeContext())
    processed_image = bias_maker.do_stage([])
    assert processed_image is None


def test_group_by_attributes():
    maker = BiasMaker(FakeContext())
    assert maker.group_by_attributes == ['configuration_mode', 'binning']


@mock.patch('banzai.utils.file_utils.make_calibration_filename_function')
def test_header_cal_type_bias(mock_namer):
    mock_namer.return_value = lambda *x: 'foo.fits'
    nx = 100
    ny = 100

    maker = BiasMaker(FakeContext())

    image_header = Header({'DATE-OBS': '2019-12-04T14:34:00',
                           'DETSEC': '[1:100,1:100]',
                           'DATASEC': '[1:100,1:100]',
                           'OBSTYPE': 'BIAS', 'RA': 0.0, 'DEC': 0.0})

    image = maker.do_stage([FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.zeros((ny, nx)),
                                                                           meta=image_header,
                                                                           bias_level=0.0)])
                             for x in range(6)])

    assert image.meta['OBSTYPE'].upper() == 'BIAS'


@mock.patch('banzai.utils.file_utils.make_calibration_filename_function')
def test_bias_level_is_average_of_inputs(mock_namer):
    mock_namer.return_value = lambda *x: 'foo.fits'
    nimages = 20
    bias_levels = np.arange(nimages, dtype=float)

    images = [FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.random.normal(i, size=(99,99)),
                                                            bias_level=i, meta=Header({'DATE-OBS': '2019-12-04T14:34:00',
                                                                                       'DETSEC': '[1:100,1:100]',
                                                                                       'DATASEC': '[1:100,1:100]',
                                                                                       'OBSTYPE': 'BIAS',
                                                                                       'RA': 0.0,
                                                                                       'DEC': 0.0}))])
              for i in bias_levels]

    fake_context = FakeContext()
    maker = BiasMaker(fake_context)
    master_bias = maker.do_stage(images)

    assert master_bias.meta['BIASLVL'] == np.mean(bias_levels)


@mock.patch('banzai.utils.file_utils.make_calibration_filename_function')
def test_makes_a_sensible_master_bias(mock_namer):
    mock_namer.return_value = lambda *x: 'foo.fits'
    nimages = 20
    expected_readnoise = 15.0

    images = [FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.random.normal(0.0, size=(99, 99), scale=expected_readnoise),
                                                            bias_level=0.0,
                                                            read_noise=expected_readnoise,
                                                            meta=Header({'DATE-OBS': '2019-12-04T14:34:00',
                                                                         'DETSEC': '[1:100,1:100]',
                                                                         'DATASEC': '[1:100,1:100]',
                                                                         'OBSTYPE': 'BIAS', 'RA': 0.0, 'DEC': 0.0}))])
              for i in range(nimages)]

    maker = BiasMaker(FakeContext())
    stacked_image = maker.do_stage(images)
    master_bias = stacked_image.data
    assert np.abs(np.mean(master_bias)) < 0.1
    actual_readnoise = np.std(master_bias)
    assert np.abs(actual_readnoise - expected_readnoise / (nimages ** 0.5)) < 0.2


def test_multiread_bias_maker():
    nimages = 5
    nreads = 20
    images = []
    pattern_scale = 8
    nx = 101
    ny = 105
    bias_level = 100.0
    bias_pattern = np.random.normal(0.0, pattern_scale, size=(ny, nx))
    for i in range(nimages):
        data = nreads * bias_pattern
        image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=data, bias_level=bias_level,
                                                              meta=Header({'DATE-OBS': '2019-12-04T14:34:00',
                                                                           'DETSEC': f'[1:{nx},1:{ny}]',
                                                                           'DATASEC': f'[1:{nx},1:{ny}]',
                                                                           'OBSTYPE': 'BIAS', 'RA': 0.0, 'DEC': 0.0}))])
        image.n_sub_exposures = nreads
        images.append(image)
    maker = BiasMaker(FakeContext())
    stacked_image = maker.do_stage(images)
    np.testing.assert_allclose(stacked_image.meta['BIASLVL'], bias_level, atol=0.1)

    # With 20 x 5 reads, we should get down to better than 1 count at a read_noise of 9
    np.testing.assert_allclose(stacked_image.data, bias_pattern, atol=1.0)
