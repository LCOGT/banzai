import pytest
import numpy as np
from astropy.table import Table
from astropy.io.fits import ImageHDU, Header
from mock import MagicMock

from banzai.utils.image_utils import Section
from banzai.data import CCDData, DataTable
from banzai.dbs import CalibrationImage
from banzai.tests.utils import FakeCCDData, FakeLCOObservationFrame, FakeContext
from banzai.lco import LCOFrameFactory, LCOObservationFrame, LCOCalibrationFrame

pytestmark = pytest.mark.frames


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(10031312)


def test_update_trimsec_fs01():
    test_header = Header({'TRIMSEC': '[11:2055,19:2031]',
                          'DATASEC': '[1:2048,1:2048]',
                          'INSTRUME': 'fs01'})
    test_hdu = ImageHDU(data=np.ones(10), header=test_header)
    LCOFrameFactory._update_fs01_sections(test_hdu)

    assert test_hdu.header.get('TRIMSEC') == '[2:2046,4:2016]'
    assert test_hdu.header.get('DATASEC') == '[10:2056,16:2032]'


def test_frame_add_hdu():
    hdu_list = [FakeCCDData(meta={'EXTNAME': 'SCI', 'OBSTYPE': 'EXPOSE'}), DataTable(data=Table(np.array([1, 2, 3])), name='CAT')]
    test_frame = LCOObservationFrame(hdu_list=hdu_list, file_path='/foo/bar')
    test_frame.add_or_update(FakeCCDData(meta={'EXTNAME': 'SCI', 'OBSTYPE': 'EXPOSE'}, name='FOO'))
    
    assert len(test_frame._hdus) == 3
    assert test_frame['FOO'].name == 'FOO'


def test_frame_duplicate_replaces_hdu():
    hdu_list = [FakeCCDData(meta={'EXTNAME': 'SCI', 'OBSTYPE': 'EXPOSE'}, name='SCI'), DataTable(data=Table(np.array([1, 2, 3])), name='CAT')]
    test_frame = LCOObservationFrame(hdu_list=hdu_list, file_path='/foo/bar')
    test_frame.add_or_update(FakeCCDData(meta={'EXTNAME': 'SCI', 'OBSTYPE': 'EXPOSE'}, name='SCI'))

    assert len(test_frame._hdus) == 2
    assert test_frame['SCI'] == hdu_list[0]


def test_frame_contains_does_not_exist():
    hdu_list = [FakeCCDData(meta={'EXTNAME': 'SCI', 'OBSTYPE': 'EXPOSE'}, name='SCI'), DataTable(data=Table(np.array([1, 2, 3])), name='CAT')]
    test_frame = LCOObservationFrame(hdu_list=hdu_list, file_path='/foo/bar')

    assert not 'FOO' in test_frame


def test_frame_to_db_record():
    hdu_list = [FakeCCDData(meta={'EXTNAME': 'SCI', 
                                  'OBSTYPE': 'BIAS', 
                                  'DATE-OBS': '2021-04-20T00:00:00.000', 
                                  'DATE': '2021-04-20T00:00:00.000',
                                  'CCDSUM': '1 1',
                                  'CONFMODE': 'full_frame'}, name='SCI')]
    test_frame = LCOCalibrationFrame(hdu_list=hdu_list, file_path='/foo/bar')
    test_frame.is_bad = False
    test_frame.frame_id = 1234
    test_frame.instrument = MagicMock(id=7)
    mock_data_product = MagicMock(filename='test.fits.fz', filepath='/path/to/test/test.fits.fz')
    db_record = test_frame.to_db_record(mock_data_product)

    assert type(db_record) == CalibrationImage
    assert db_record.is_master == False
    assert db_record.type == 'BIAS'
    assert db_record.frameid == 1234


def test_ccd_data_to_fits():
    test_data = FakeCCDData(meta={}, name='SCI')
    hdu_list = test_data.to_fits(FakeContext())
    assert len(hdu_list) == 3
    assert hdu_list[0].header['EXTNAME'] == 'SCI'
    assert hdu_list[1].header['EXTNAME'] == 'BPM'
    assert hdu_list[2].header['EXTNAME'] == 'ERR'


def test_exposure_to_fits_reorder_fpack():
    hdu_list = [FakeCCDData(meta={'OBSTYPE': 'EXPOSE'}, name='SCI'), DataTable(data=Table(np.array([1, 2, 3])), name='CAT')]
    test_frame = FakeLCOObservationFrame(hdu_list=hdu_list)
    context = FakeContext()
    context.fpack = True
    assert [hdu.header.get('EXTNAME') for hdu in test_frame.to_fits(context)] == [None, 'SCI', 'CAT', 'BPM', 'ERR']


def test_exposure_to_fits_reorder_fpack_missing_cat():
    hdu_list = [FakeCCDData(meta={'OBSTYPE': 'EXPOSE'}, name='SCI')]
    test_frame = FakeLCOObservationFrame(hdu_list=hdu_list)
    context = FakeContext()
    context.fpack = True
    assert [hdu.header.get('EXTNAME') for hdu in test_frame.to_fits(context)] == [None, 'SCI', 'BPM', 'ERR']


def test_exposure_to_fits_reorder_no_fpack():
    hdu_list = [FakeCCDData(meta={'OBSTYPE': 'EXPOSE'}, name='SCI'), DataTable(data=Table(np.array([1, 2, 3])), name='CAT')]
    test_frame = FakeLCOObservationFrame(hdu_list=hdu_list)
    context = FakeContext()
    context.fpack = False
    assert [hdu.header.get('EXTNAME') for hdu in test_frame.to_fits(context)] == ['SCI', 'CAT', 'BPM', 'ERR']


def test_calibration_to_fits_reorder_fpack():
    hdu_list = [FakeCCDData(meta={'OBSTYPE': 'BIAS'}, name='SCI')]
    test_frame = FakeLCOObservationFrame(hdu_list=hdu_list)
    test_frame.hdu_order = ['SCI', 'BPM', 'ERR']
    context = FakeContext()
    context.fpack = True
    assert [hdu.header.get('EXTNAME') for hdu in test_frame.to_fits(context)] == [None, 'SCI', 'BPM', 'ERR']


def test_calibration_to_fits_reorder_no_fpack():
    hdu_list = [FakeCCDData(meta={'OBSTYPE': 'BIAS'}, name='SCI')]
    test_frame = FakeLCOObservationFrame(hdu_list=hdu_list)
    test_frame.hdu_order = ['SCI', 'BPM', 'ERR']
    context = FakeContext()
    context.fpack = False
    assert [hdu.header.get('EXTNAME') for hdu in test_frame.to_fits(context)] == ['SCI', 'BPM', 'ERR']


def test_all_datatypes_wrong():
    hdu_list = [FakeCCDData(data=np.ones((2,2), dtype=np.float64), name='SCI'),
                FakeCCDData(data=np.ones((2,2), dtype=np.float64), name='BPM'),
                FakeCCDData(data=np.ones((2,2), dtype=np.float64), name='ERR')]
    test_frame = FakeLCOObservationFrame(hdu_list=hdu_list)
    test_frame.hdu_order = ['SCI', 'BPM', 'ERR']
    context = FakeContext()
    context.fpack = False
    output_hdu_list = test_frame.to_fits(context)

    assert output_hdu_list['SCI'].data.dtype == np.float32
    assert output_hdu_list['BPM'].data.dtype == np.uint8
    assert output_hdu_list['ERR'].data.dtype == np.float32


def test_subtract():
    test_data = FakeCCDData(image_multiplier=4, uncertainty=3)
    test_data -= 1

    assert (test_data.data == 3 * np.ones(test_data.data.shape)).all()
    assert np.allclose(test_data.uncertainty, 3)


def test_uncertainty_propagation_on_divide():
    data1 = FakeCCDData(data=np.array([4.0, 4.0]), uncertainty=np.array([2.0, 2.0]), meta={'SATURATE': 6, 'GAIN': 4, 'MAXLIN': 2})
    data2 = FakeCCDData(data=np.array([2.0, 2.0]), uncertainty=np.array([2.0, 2.0]), meta={'SATURATE': 3, 'GAIN': 2, 'MAXLIN': 1})
    data1 /= data2
    assert np.allclose(data1.data, 2.0)
    assert np.allclose(data1.uncertainty, np.sqrt(5))


def test_uncertainty_propagation_on_subtract():
    data1 = FakeCCDData(data=np.array([2, 2]), uncertainty=np.sqrt(np.array([2, 2])))
    data2 = FakeCCDData(data=np.array([1, 1]), uncertainty=np.sqrt(np.array([2, 2])))
    data1 -= data2
    assert np.allclose(data1.data, 1)
    assert np.allclose(data1.uncertainty, 2)


def test_trim():
    test_data = FakeCCDData(nx=1000, ny=1000,
                            meta={'TRIMSEC': '[1:950, 1:945]',
                                  'DATASEC': '[1:1000, 1:1000]',
                                  'DETSEC': '[1:1000, 1:1000]'},
                            memmap=False)

    trimmed_data = test_data.trim()

    assert trimmed_data.data.shape == (945, 950)
    assert trimmed_data.mask.shape == (945, 950)
    assert trimmed_data.uncertainty.shape == (945, 950)


def test_init_poisson_uncertainties():
    # Make sure the uncertainties add in quadrature
    test_data = FakeCCDData(image_multiplier=16, uncertainty=3)
    test_data.init_poisson_uncertainties()
    assert (test_data.uncertainty == 5 * np.ones(test_data.data.shape)).all()


def test_get_output_filename():
    test_frame = FakeLCOObservationFrame(file_path='test_image_00.fits')
    test_context = FakeContext(frame_class=FakeLCOObservationFrame)
    filename = test_frame.get_output_filename(test_context)

    assert filename == 'test_image_91.fits.fz'


def test_section_transformation():
    nx = 1024
    ny = 1024
    datasec = '[1:1024,1:1024]'
    detsec = '[1:1024,1:1024]'
    test_image = CCDData(np.zeros((ny, nx)), {'CCDSUM': '1 1'})
    test_image._data_section = Section.parse_region_keyword(datasec)
    test_image.detector_section = Section.parse_region_keyword(detsec)

    test_sections = {'DATASEC': Section.parse_region_keyword('[1:1024, 1:1024]'),
                     'DETSEC': Section.parse_region_keyword('[1:1024, 1:1024]')}

    assert test_image.data_to_detector_section(test_sections['DATASEC']).to_region_keyword() == test_sections['DETSEC'].to_region_keyword()
    assert test_image.detector_to_data_section(test_sections['DETSEC']).to_region_keyword() == test_sections['DATASEC'].to_region_keyword()


def test_section_transformation_with_binning():
    nx = 1024
    ny = 1024
    datasec = '[1:1024,1:1024]'
    detsec = '[1:2048,1:2048]'
    test_image = CCDData(np.zeros((ny, nx)), {'CCDSUM': '2 2'})
    test_image._data_section = Section.parse_region_keyword(datasec)
    test_image.detector_section = Section.parse_region_keyword(detsec)

    test_sections = {'DATASEC': Section.parse_region_keyword('[1:1024, 1:1024]'),
                     'DETSEC': Section.parse_region_keyword('[1:2048, 1:2048]')}

    assert test_image.data_to_detector_section(test_sections['DATASEC']).to_region_keyword() == test_sections['DETSEC'].to_region_keyword()
    assert test_image.detector_to_data_section(test_sections['DETSEC']).to_region_keyword() == test_sections['DATASEC'].to_region_keyword()


def test_section_transformation_with_binning_small_2():
    nx = 1024
    ny = 1024
    datasec = '[1:1024,1:1024]'
    detsec = '[1:2048,1:2048]'
    test_image = CCDData(np.zeros((ny, nx)), {'CCDSUM': '2 2'})
    test_image._data_section = Section.parse_region_keyword(datasec)
    test_image.detector_section = Section.parse_region_keyword(detsec)

    test_sections = {'DATASEC': Section.parse_region_keyword('[3:7, 1:1024]'),
                     'DETSEC': Section.parse_region_keyword('[5:14, 1:2048]')}

    assert test_image.data_to_detector_section(test_sections['DATASEC']).to_region_keyword() == test_sections['DETSEC'].to_region_keyword()
    assert test_image.detector_to_data_section(test_sections['DETSEC']).to_region_keyword() == test_sections['DATASEC'].to_region_keyword()


def test_section_transformation_with_binning_small_3():
    nx = 1024
    ny = 1024
    datasec = '[1:1024,1:1024]'
    detsec = '[1:3072,1:3072]'
    test_image = CCDData(np.zeros((ny, nx)), {'CCDSUM': '3 3'})
    test_image._data_section = Section.parse_region_keyword(datasec)
    test_image.detector_section = Section.parse_region_keyword(detsec)

    test_sections = {'DATASEC': Section.parse_region_keyword('[3:4, 1:1024]'),
                     'DETSEC': Section.parse_region_keyword('[7:12, 1:3072]')}

    assert test_image.data_to_detector_section(test_sections['DATASEC']).to_region_keyword() == test_sections['DETSEC'].to_region_keyword()
    assert test_image.detector_to_data_section(test_sections['DETSEC']).to_region_keyword() == test_sections['DATASEC'].to_region_keyword()


def test_section_transformation_with_offset_datasec():
    nx = 1048
    ny = 1048
    datasec = '[25:1048,25:1048]'
    detsec = '[1:1024,1:1024]'
    test_image = CCDData(np.zeros((ny, nx)), {'CCDSUM': '1 1'})
    test_image._data_section = Section.parse_region_keyword(datasec)
    test_image.detector_section = Section.parse_region_keyword(detsec)

    test_sections = {'DATASEC': Section.parse_region_keyword('[25:1048, 25:1048]'),
                     'DETSEC': Section.parse_region_keyword('[1:1024, 1:1024]')}

    assert test_image.data_to_detector_section(test_sections['DATASEC']).to_region_keyword() == test_sections['DETSEC'].to_region_keyword()
    assert test_image.detector_to_data_section(test_sections['DETSEC']).to_region_keyword() == test_sections['DATASEC'].to_region_keyword()


def test_section_transformation_with_offset_datasec_with_binning():
    nx = 1048
    ny = 1048
    datasec = '[25:1048,25:1048]'
    detsec = '[1:2048,1:2048]'
    test_image = CCDData(np.zeros((ny, nx)), {'CCDSUM': '2 2'})
    test_image._data_section = Section.parse_region_keyword(datasec)
    test_image.detector_section = Section.parse_region_keyword(detsec)

    test_sections = {'DATASEC': Section.parse_region_keyword('[25:1048, 25:1048]'),
                     'DETSEC': Section.parse_region_keyword('[1:2048, 1:2048]')}

    assert test_image.data_to_detector_section(test_sections['DATASEC']).to_region_keyword() == test_sections['DETSEC'].to_region_keyword()
    assert test_image.detector_to_data_section(test_sections['DETSEC']).to_region_keyword() == test_sections['DATASEC'].to_region_keyword()


def test_detector_to_data_section_flipped():
    nx = 1024
    ny = 1024
    datasec = '[1:1024,1:1024]'
    detsec = '[1024:1,1024:1]'
    test_image = CCDData(np.zeros((ny, nx)), {'CCDSUM': '1 1'})
    test_image._data_section = Section.parse_region_keyword(datasec)
    test_image.detector_section = Section.parse_region_keyword(detsec)

    requested_detsec = '[1:1024,1:1024]'
    expected_datasec = '[1024:1,1024:1]'

    requested_detsec = Section.parse_region_keyword(requested_detsec)
    assert expected_datasec == test_image.detector_to_data_section(requested_detsec).to_region_keyword()


def test_detector_to_data_section_with_binning_small_3_flipped():
    nx = 1024
    ny = 1024
    datasec = '[1:1024,1:1024]'
    detsec = '[1:3072,1:3072]'
    test_image = CCDData(np.zeros((ny, nx)), {'CCDSUM': '3 3'})
    test_image._data_section = Section.parse_region_keyword(datasec)
    test_image.detector_section = Section.parse_region_keyword(detsec)

    requested_detsec = '[12:7,1:3072]'
    expected_datasec = '[4:3,1:1024]'

    requested_detsec = Section.parse_region_keyword(requested_detsec)
    assert expected_datasec == test_image.detector_to_data_section(requested_detsec).to_region_keyword()


def test_detector_to_data_section_2k_binned():
    headers = [{'DATASEC': '[1:512,1:512]', 'DETSEC': '[1025:2048,3072:2049]', 'CCDSUM': '2 2'},
               {'DATASEC': '[1:512,1:512]', 'DETSEC': '[1025:2048,1025:2048]', 'CCDSUM': '2 2'},
               {'DATASEC': '[1:512,1:512]', 'DETSEC': '[3072:2049,1025:2048]', 'CCDSUM': '2 2'},
               {'DATASEC': '[1:512,1:512]', 'DETSEC': '[3072:2049,3072:2049]', 'CCDSUM': '2 2'}]
    for header in headers:
        test_data = CCDData(np.zeros((512, 512)), meta=header)
        detector_section = Section.parse_region_keyword(header['DETSEC'])
        assert test_data.detector_to_data_section(detector_section).to_region_keyword() == header['DATASEC']


def test_detector_to_data_section_full():
    cases = [{'DATASEC': '1:1024', 'DETSEC': '1:1024', 'request': '51:100', 'result': '51:100'},
             {'DATASEC': '1:1024', 'DETSEC': '1:1024', 'request': '100:51', 'result': '100:51'},
             {'DATASEC': '1024:1', 'DETSEC': '1024:1', 'request': '51:100', 'result': '51:100'},
             {'DATASEC': '1024:1', 'DETSEC': '1024:1', 'request': '100:51', 'result': '100:51'},
             {'DATASEC': '1:1024', 'DETSEC': '1024:1', 'request': '51:100', 'result': '974:925'},
             {'DATASEC': '1:1024', 'DETSEC': '1024:1', 'request': '100:51', 'result': '925:974'},
             {'DATASEC': '1024:1', 'DETSEC': '1:1024', 'request': '51:100', 'result': '974:925'},
             {'DATASEC': '1024:1', 'DETSEC': '1:1024', 'request': '100:51', 'result': '925:974'}]
    for y_case in cases:
        for x_case in cases:
            test_data = CCDData(np.zeros((1024, 1024)), meta={'DATASEC': f'[{x_case["DATASEC"]},{y_case["DATASEC"]}]',
                                                              'DETSEC': f'[{x_case["DETSEC"]},{y_case["DETSEC"]}]',
                                                              'CCDSUM': '1 1'})
            requested_section = Section.parse_region_keyword(f'[{x_case["request"]},{ y_case["request"]}]')
            assert test_data.detector_to_data_section(requested_section).to_region_keyword() == f'[{x_case["result"]},{y_case["result"]}]'


def test_data_to_detector_section_2k_binned():
    headers = [{'DATASEC': '[1:512,1:512]', 'DETSEC': '[1025:2048,3072:2049]', 'CCDSUM': '2 2'},
               {'DATASEC': '[1:512,1:512]', 'DETSEC': '[1025:2048,1025:2048]', 'CCDSUM': '2 2'},
               {'DATASEC': '[1:512,1:512]', 'DETSEC': '[3072:2049,1025:2048]', 'CCDSUM': '2 2'},
               {'DATASEC': '[1:512,1:512]', 'DETSEC': '[3072:2049,3072:2049]', 'CCDSUM': '2 2'}]
    for header in headers:
        test_data = CCDData(np.zeros((512, 512)), meta=header)
        data_section = Section.parse_region_keyword(header['DATASEC'])
        assert test_data.data_to_detector_section(data_section).to_region_keyword() == header['DETSEC']


def test_data_to_detector_section_full():
    cases = [{'DATASEC': '1:1024', 'DETSEC': '1:1024', 'request': '51:100', 'result': '51:100'},
             {'DATASEC': '1:1024', 'DETSEC': '1:1024', 'request': '100:51', 'result': '100:51'},
             {'DATASEC': '1024:1', 'DETSEC': '1024:1', 'request': '51:100', 'result': '51:100'},
             {'DATASEC': '1024:1', 'DETSEC': '1024:1', 'request': '100:51', 'result': '100:51'},
             {'DATASEC': '1:1024', 'DETSEC': '1024:1', 'request': '51:100', 'result': '974:925'},
             {'DATASEC': '1:1024', 'DETSEC': '1024:1', 'request': '100:51', 'result': '925:974'},
             {'DATASEC': '1024:1', 'DETSEC': '1:1024', 'request': '51:100', 'result': '974:925'},
             {'DATASEC': '1024:1', 'DETSEC': '1:1024', 'request': '100:51', 'result': '925:974'}]
    for y_case in cases:
        for x_case in cases:
            test_data = CCDData(np.zeros((1024, 1024)), meta={'DATASEC': f'[{x_case["DATASEC"]},{y_case["DATASEC"]}]',
                                                              'DETSEC': f'[{x_case["DETSEC"]},{y_case["DETSEC"]}]',
                                                              'CCDSUM': '1 1'})
            requested_section = Section.parse_region_keyword(f'[{x_case["result"]},{ y_case["result"]}]')
            assert test_data.data_to_detector_section(requested_section).to_region_keyword() == f'[{x_case["request"]},{y_case["request"]}]'


def test_propid_public():
    proposal_ids = ['standard', 'Photometric standards', 'NRES standards', 'FLOYDS standards']
    date_obs = '2021-09-01T00:00:00'
    test_data = [CCDData(np.zeros((1024, 1024)), meta={'PROPID': propid,
                                                       'DATE-OBS': date_obs}) for propid in proposal_ids]

    test_frames = [LCOObservationFrame([data], file_path='/tmp') for data in test_data]
    context = FakeContext()

    for frame in test_frames:
        frame.save_processing_metadata(context)

        assert frame.meta['L1PUBDAT'] == (date_obs, '[UTC] Date the frame becomes public')


def test_propid_not_public():
    date_obs = '2021-09-01T00:00:00'
    test_data = CCDData(np.zeros((1024, 1024)), meta={'PROPID': 'Non-public-proposal',
                                                      'DATE-OBS': date_obs})

    test_frame = LCOObservationFrame([test_data], file_path='/tmp')
    context = FakeContext()

    test_frame.save_processing_metadata(context)

    assert test_frame.meta['L1PUBDAT'] != (date_obs, '[UTC] Date the frame becomes public')
