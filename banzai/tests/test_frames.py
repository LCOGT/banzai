import pytest
import numpy as np

from banzai.utils.image_utils import Section
from banzai.data import CCDData
from banzai.tests.utils import FakeCCDData, FakeLCOObservationFrame, FakeContext

pytestmark = pytest.mark.frames


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(10031312)


def test_to_fits():
    test_data = FakeCCDData(meta={'EXTNAME': 'SCI'})
    hdu_list = test_data.to_fits(FakeContext())
    assert len(hdu_list) == 3
    assert hdu_list[0].header['EXTNAME'] == 'SCI'
    assert hdu_list[1].header['EXTNAME'] == 'BPM'
    assert hdu_list[2].header['EXTNAME'] == 'ERR'


def test_subtract():
    test_data = FakeCCDData(image_multiplier=4, uncertainty=3)
    test_data -= 1

    assert (test_data.data == 3 * np.ones(test_data.data.shape)).all()
    assert np.allclose(test_data.uncertainty, 3)


def test_uncertainty_propagation_on_divide():
    data1 = FakeCCDData(data=np.array([2, 2]), uncertainty=np.array([2, 2]))
    data2 = FakeCCDData(data=np.array([1, 1]), uncertainty=np.array([2, 2]))
    data1 /= data2
    assert np.allclose(data1.data, 2)
    assert np.allclose(data1.uncertainty, 2*np.sqrt(5))


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
    test_data = FakeCCDData(image_multiplier=4, uncertainty=1)
    test_data.init_poisson_uncertainties()
    assert (test_data.uncertainty == 3 * np.ones(test_data.data.shape)).all()


def test_get_output_filename():
    test_frame = FakeLCOObservationFrame(file_path='test_image_00.fits')
    test_context = FakeContext(frame_class=FakeLCOObservationFrame)
    filename = test_frame.get_output_filename(test_context)

    assert filename == '/tmp/cpt/fa16/20160101/processed/test_image_91.fits.fz'


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
