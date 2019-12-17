import mock
import numpy as np
import pytest
from astropy.io.fits import Header

from banzai.bias import BiasMaker
from banzai.tests.utils import FakeContext, FakeLCOObservationFrame, FakeCCDData
from banzai.tests.bias_utils import FakeBiasImage

import pdb

pytestmark = pytest.mark.bias_maker


def test_min_images():
    bias_maker = BiasMaker(FakeContext())
    processed_images = bias_maker.do_stage([])
    assert len(processed_images) == 0


def test_group_by_attributes():
    maker = BiasMaker(FakeContext())
    assert maker.group_by_attributes() == ['configuration_mode', 'binning']


@mock.patch('banzai.images.LCOFrameFactory.open')
@mock.patch('banzai.utils.file_utils.make_calibration_filename_function')
def test_header_cal_type_bias(mock_namer, mock_master_frame):
    mock_namer.return_value = lambda *x: 'foo.fits'

    master_readnoise = 10.0
    master_bias_level = 0.0
    nx = 100
    ny = 100

    fake_master_image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.random.normal(master_bias_level, master_readnoise,
                                                                                            size=(ny, nx)),
                                                read_noise=master_readnoise,
                                                bias_level=master_bias_level)])

    mock_master_frame.return_value = fake_master_image

    maker = BiasMaker(FakeContext())

    image_header = Header({'DATE-OBS': '2019-12-04T14:34:00',
                           'DETSEC': '[1:100,1:100]',
                           'DATASEC': '[1:100,1:100]',
                           'OBSTYPE': 'BIAS'})

    images = maker.do_stage([FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.zeros((ny, nx)),
                                                                           meta=image_header)])
                             for x in range(6)])

    assert images[0].meta['OBSTYPE'].upper() == 'BIAS'


@mock.patch('banzai.utils.file_utils.make_calibration_filename_function')
def test_bias_level_is_average_of_inputs(mock_namer):
    mock_namer.return_value = lambda *x: 'foo.fits'
    nimages = 20
    bias_levels = np.arange(nimages, dtype=float)
    image_header = Header({'DATE-OBS': '2019-12-04T14:34:00',
                           'DETSEC': '[1:100,1:100]',
                           'DATASEC': '[1:100,1:100]',
                           'OBSTYPE': 'BIAS'})

    # images = []
    # for level in bias_levels:
    #     # print(level)
    #     # image = FakeCCDData(bias_level=level, meta=image_header)
    #     # image.meta['BIASLVL'] = level
    #     # print(level)
    #     image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(bias_level=level, meta=image_header)])
    #     images.append(image)
    #     # images.append(FakeLCOObservationFrame(hdu_list=[FakeCCDData(bias_level=level, meta=image_header)]))
    # pdb.set_trace()
    #
    # images.append(FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.random.normal(1, size=(99,99)),
    #                                                             bias_level=1, meta=Header({'DATE-OBS': '2019-12-04T14:34:00',
    #                        'DETSEC': '[1:100,1:100]',
    #                        'DATASEC': '[1:100,1:100]',
    #                        'OBSTYPE': 'BIAS'}))]))
    #
    # images.append(FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.random.normal(2, size=(99,99)),
    #                                                             bias_level=2, meta=image_header)]))
    #
    # images.append(FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.random.normal(2, size=(99,99)),
    #                                                             bias_level=2, meta=image_header)]))
    #
    # images.append(FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.random.normal(2, size=(99,99)),
    #                                                             bias_level=2, meta=image_header)]))
    #
    # images.append(FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.random.normal(2, size=(99,99)),
    #                                                             bias_level=2, meta=image_header)]))



    # pdb.set_trace()
    images = [FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.random.normal(i, size=(99,99)),
                                                            bias_level=i, meta=Header({'DATE-OBS': '2019-12-04T14:34:00',
                                                                                       'DETSEC': '[1:100,1:100]',
                                                                                       'DATASEC': '[1:100,1:100]',
                                                                                       'OBSTYPE': 'BIAS'}))]) for i in bias_levels]
    # pdb.set_trace()
    #
    # for idx, image in enumerate(images):
    #     image.bias_level = bias_levels[idx]
    #     print(image.bias_level)
    #     print(image.primary_hdu.meta['BIASLVL'])
    #
    # pdb.set_trace()
    # mock_instrument_info.return_value = None, None, None
    fake_context = FakeContext()
    # fake_context.db_address = ''
    # pdb.set_trace()
    maker = BiasMaker(fake_context)
    pdb.set_trace()
    master_bias = maker.do_stage(images)[0]

    assert master_bias.meta['BIASLVL'] == np.mean(bias_levels)


@mock.patch('banzai.utils.file_utils.make_calibration_filename_function')
@mock.patch('banzai.calibrations.FRAME_CLASS', side_effect=FakeBiasImage)
def test_makes_a_sensible_master_bias(mock_frame, mock_namer):
    mock_namer.return_value = lambda *x: 'foo.fits'
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
