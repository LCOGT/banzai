import tempfile
from unittest.mock import patch

import pytest
import numpy as np
from astropy.io.fits import Header

from banzai.data import CCDData, HeaderOnly
from banzai.lco import LCOObservationFrame
from banzai.utils.image_utils import Section
from banzai.mosaic import MosaicCreator
from banzai.tests.utils import FakeLCOObservationFrame, FakeCCDData

pytestmark = pytest.mark.mosaic_creator

extension_headers = [{'SATURATE': 35000, 'MAXLIN': 35000, 'GAIN': 1.0, 'DATASEC': '[1:512,1:512]', 'DETSEC': '[1025:2048,3072:2049]', 'CCDSUM': '2 2', 'OVERSCAN': 8000},
                     {'SATURATE': 35000, 'MAXLIN': 35000, 'GAIN': 1.0, 'DATASEC': '[1:512,1:512]', 'DETSEC': '[1025:2048,1025:2048]', 'CCDSUM': '2 2', 'OVERSCAN': 8100},
                     {'SATURATE': 35000, 'MAXLIN': 35000, 'GAIN': 1.0, 'DATASEC': '[1:512,1:512]', 'DETSEC': '[3072:2049,1025:2048]', 'CCDSUM': '2 2', 'OVERSCAN': 8050},
                     {'SATURATE': 35000, 'MAXLIN': 35000, 'GAIN': 1.0, 'DATASEC': '[1:512,1:512]', 'DETSEC': '[3072:2049,3072:2049]', 'CCDSUM': '2 2', 'OVERSCAN': 8235}]

expected_overscans = [(i, 'Overscan value that was subtracted') for i in ['8000.00', '8100.00', '8050.00', '8235.00']]


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(200)


def test_null_input_image():
    mosaic_creator = MosaicCreator(None)
    image = mosaic_creator.run(None)
    assert image is None


def test_get_mosaic_size():
    detsecs = [['[1:100,1:100]', '[1:100,200:101]', '[200:101,1:100]', '[200:101,200:101]'],
               ['[1:200,400:201]', '[1:200,1:200]', '[400:201,400:201]', '[400:201,1:200]'],
               ['[600:301,600:301]', '[600:301,1:300]', '[1:300,1:300]', '[1:300,600:301]'],
               ['[800:401,1:400]', '[800:401,800:401]', '[1:400,800:401]', '[1:400,1:400]'],]
    expected_mosaic_sizes = [(200, 200), (400, 400), (600, 600), (800, 800), (800, 800)]

    for idx, detsec in enumerate(detsecs):
        test_data = [FakeCCDData(meta={'CCDSUM': '1 1', 'DETSEC': detsec[amp]}) for amp in range(0, len(detsec))]
        test_frame = FakeLCOObservationFrame(hdu_list=test_data)
        assert MosaicCreator.get_mosaic_detector_region(test_frame).shape == expected_mosaic_sizes[idx]


def test_get_mosaic_detector_region():
    data = [FakeCCDData(meta=extension_header) for extension_header in extension_headers]
    image = FakeLCOObservationFrame(hdu_list=data)
    assert MosaicCreator.get_mosaic_detector_region(image).shape == (2048, 2048)


def make_single_component_frame(detsec='[1:6,1:4]', datasec='[1:6,1:4]', binning='1 1',
                                separate_primary=False, uncertainty_dtype=np.float64):
    meta = Header({'OBSTYPE': 'SUB_EXP', 'DAY-OBS': '20260915', 'MOLUID': 123, 'MOLFRNUM': 2,
                   'FRMTOTAL': 10, 'GAIN': 1.0, 'SATURATE': 40000.0, 'MAXLIN': 30000.0,
                   'RDNOISE': 3.0, 'CCDSUM': binning, 'DATASEC': datasec, 'DETSEC': detsec,
                   'TRIMSEC': '[1:6,1:4]', 'OVERSCAN': 3.25, 'L1STATOV': '1'})
    pixels = np.arange(24, dtype=np.float64).reshape(4, 6)
    component = CCDData(data=pixels, meta=meta, mask=pixels.astype(np.uint8) % 16,
                        uncertainty=(pixels / 10 + 1).astype(uncertainty_dtype), name='RAW')
    hdus = [component]
    if separate_primary:
        primary_meta = meta.copy()
        primary_meta.update({'GAIN': 2.0, 'SATURATE': 50000.0, 'MAXLIN': 45000.0,
                             'L1STATOV': '0', 'OVERSCAN': 0.0, 'PRIMARY': 'preserve'})
        component.meta['AMPONLY'] = 'omit'
        hdus.insert(0, HeaderOnly(meta=primary_meta, name='PRIMARY'))
    return LCOObservationFrame(hdu_list=hdus, file_path='/tmp/single-component.fits')


@pytest.mark.parametrize(('separate_primary', 'detsec', 'binning', 'uncertainty_dtype'), [
    (False, '[1:6,1:4]', '1 1', np.float64),
    (True, '[1:6,1:4]', '1 1', np.float64),
    (True, '[101:112,201:208]', '2 2', np.float64),
    (True, '[1:6,1:4]', '1 1', np.float32),
])
def test_single_component_reuses_arrays_and_matches_general_mosaic(
        monkeypatch, separate_primary, detsec, binning, uncertainty_dtype):
    kwargs = dict(separate_primary=separate_primary, detsec=detsec, binning=binning,
                  uncertainty_dtype=uncertainty_dtype)
    with monkeypatch.context() as general_path:
        general_path.setattr(MosaicCreator, '_can_reuse_component', staticmethod(lambda *args: False))
        expected = MosaicCreator(None).do_stage(make_single_component_frame(**kwargs))

    image = make_single_component_frame(**kwargs)
    component = image.ccd_hdus[0]
    original_meta = component.meta.copy()
    with patch('banzai.data.tempfile.NamedTemporaryFile', wraps=tempfile.NamedTemporaryFile) as new_file:
        actual = MosaicCreator(None).do_stage(image)

    assert actual is image
    assert len(actual.ccd_hdus) == 1
    assert actual.primary_hdu.name == 'SCI'
    assert actual.meta == expected.meta
    assert component.meta == original_meta
    assert component.memmap is True
    assert actual.primary_hdu.memmap is True
    assert actual.data is component.data
    assert actual.mask is component.mask
    if uncertainty_dtype == np.float64:
        assert actual.uncertainty is component.uncertainty
        assert new_file.call_count == 0
    else:
        assert actual.uncertainty is not component.uncertainty
        assert new_file.call_count == 1
    for name in ('data', 'mask', 'uncertainty'):
        result_array = getattr(actual.primary_hdu, name)
        expected_array = getattr(expected.primary_hdu, name)
        assert result_array.dtype == expected_array.dtype
        np.testing.assert_array_equal(result_array, expected_array)


@pytest.mark.parametrize(('detsec', 'datasec', 'expected_slice'), [
    ('[6:1,1:4]', '[1:6,1:4]', (slice(None), slice(None, None, -1))),
    ('[1:6,1:4]', '[6:1,1:4]', (slice(None), slice(None, None, -1))),
    ('[1:4,1:4]', '[2:5,1:4]', (slice(None), slice(1, 5))),
])
def test_single_component_still_crops_and_flips(detsec, datasec, expected_slice):
    image = make_single_component_frame(detsec=detsec, datasec=datasec)
    component = image.ccd_hdus[0]
    actual = MosaicCreator(None).do_stage(image)

    assert actual.data is not component.data
    for name in ('data', 'mask', 'uncertainty'):
        np.testing.assert_array_equal(getattr(actual.primary_hdu, name), getattr(component, name)[expected_slice])


def test_mosaic_maker(set_random_seed):
    detsecs = [['[1:100,1:100]', '[1:100,200:101]', '[200:101,1:100]', '[200:101,200:101]'],
               ['[1:200,400:201]', '[1:200,1:200]', '[400:201,400:201]', '[400:201,1:200]'],
               ['[600:301,600:301]', '[600:301,1:300]', '[1:300,1:300]', '[1:300,600:301]'],
               ['[800:401,1:400]', '[800:401,800:401]', '[1:400,800:401]', '[1:400,1:400]']]
    datasecs = ['[1:100,1:100]', '[1:200,1:200]', '[1:300,1:300]', '[1:400,1:400]']

    expected_mosaic_sizes = [(200, 200), (400, 400), (600, 600), (800, 800)]
    expected_quad_slices = [[(slice(0, 100), slice(0, 100)), (slice(199, 99, -1), slice(0, 100)),
                             (slice(0, 100), slice(199, 99, -1)), (slice(199, 99, -1), slice(199, 99, -1))],
                            [(slice(399, 199, -1), slice(0, 200)), (slice(0, 200), slice(0, 200)),
                             (slice(399, 199, -1), slice(399, 199, -1)), (slice(0, 200), slice(399, 199, -1))],
                            [(slice(599, 299, -1), slice(599, 299, -1)), (slice(0, 300), slice(599, 299, -1)),
                             (slice(0, 300), slice(0, 300)), (slice(599, 299, -1), slice(0, 300))],
                            [(slice(0, 400), slice(799, 399, -1)), (slice(799, 399, -1), slice(799, 399, -1)),
                             (slice(799, 399, -1), slice(0, 400)), (slice(0, 400), slice(0, 400))]]
    data_sizes = [(4, 100, 100), (4, 200, 200), (4, 300, 300), (4, 400, 400)]
    data_arrays = []
    bpm_arrays = []
    fake_images = []

    # Create 4 images, each with 4 extensions
    # Each image will have the same 4 datasecs, but the detsecs will differ from image-to-image
    for i, detsec in enumerate(detsecs):
        extension_data = np.random.uniform(0, 1, size=data_sizes[i])
        extension_masks = np.random.choice([0, 1], size=data_sizes[i])

        data_arrays.append(extension_data)
        bpm_arrays.append(extension_masks)

        hdu_list = [FakeCCDData(data=data.copy(),
                                meta={'SATURATE': 35000, 'MAXLIN': 35000, 'GAIN': 1.0, 'OVERSCAN': 8100},
                                mask=mask.copy(), memmap=False) for data, mask in zip(extension_data, extension_masks)]

        for j in range(4):
            hdu_list[j].detector_section = Section.parse_region_keyword(detsec[j])
            hdu_list[j].data_section = Section.parse_region_keyword(datasecs[i])

        image = FakeLCOObservationFrame(hdu_list=hdu_list)
        fake_images.append(image)

    mosaic_creator = MosaicCreator(None)
    mosaiced_images = [mosaic_creator.do_stage(fake_image) for fake_image in fake_images]

    for i, image in enumerate(mosaiced_images):
        assert image.data.shape == expected_mosaic_sizes[i]
        for j, s in enumerate(expected_quad_slices[i]):
            np.testing.assert_allclose(image.data[s], data_arrays[i][j])
            np.testing.assert_allclose(image.mask[s], bpm_arrays[i][j])


def test_mosaic_maker_for_binned_windowed_mode():
    extension_data = [np.random.uniform(0, 1, size=(512,512)) for i in range(4)]
    extension_masks = [np.random.choice([0, 1], size=(512,512)) for i in range(4)]
    hdu_list = [FakeCCDData(meta=header,
                            data=data.copy(),
                            mask=mask.copy(),
                            memmap=False) for header, data, mask in zip(extension_headers, extension_data, extension_masks)]

    image = FakeLCOObservationFrame(hdu_list=hdu_list)
    expected_quad_slices = [(slice(1023, 511, -1), slice(0, 512)), (slice(0, 512), slice(0, 512)),
                            (slice(0, 512), slice(1023, 511, -1)), (slice(1023, 511, -1), slice(1023, 511, -1))]

    mosaic_creator = MosaicCreator(None)
    mosaiced_image = mosaic_creator.do_stage(image)

    assert mosaiced_image.data.shape == (1024, 1024)
    for i in range(4):
        assert mosaiced_image.meta[f'OVERSCN{i + 1}'] == expected_overscans[i]

    for j, s in enumerate(expected_quad_slices):
        np.testing.assert_allclose(mosaiced_image.data[s], extension_data[j])
        np.testing.assert_allclose(mosaiced_image.mask[s], extension_masks[j])
