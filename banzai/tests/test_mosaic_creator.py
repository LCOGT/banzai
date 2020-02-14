import pytest
import numpy as np

from banzai.utils.image_utils import Section
from banzai.mosaic import MosaicCreator
from banzai.tests.utils import FakeLCOObservationFrame, FakeCCDData

pytestmark = pytest.mark.mosaic_creator

extension_headers = [{'SATURATE': 35000, 'MAXLIN': 35000, 'GAIN': 1.0, 'DATASEC': '[1:512,1:512]', 'DETSEC': '[1025:2048,3072:2049]', 'CCDSUM': '2 2'},
                     {'SATURATE': 35000, 'MAXLIN': 35000, 'GAIN': 1.0, 'DATASEC': '[1:512,1:512]', 'DETSEC': '[1025:2048,1025:2048]', 'CCDSUM': '2 2'},
                     {'SATURATE': 35000, 'MAXLIN': 35000, 'GAIN': 1.0, 'DATASEC': '[1:512,1:512]', 'DETSEC': '[3072:2049,1025:2048]', 'CCDSUM': '2 2'},
                     {'SATURATE': 35000, 'MAXLIN': 35000, 'GAIN': 1.0, 'DATASEC': '[1:512,1:512]', 'DETSEC': '[3072:2049,3072:2049]', 'CCDSUM': '2 2'}]


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
                                meta={'SATURATE': 35000, 'MAXLIN': 35000, 'GAIN': 1.0},
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
    for j, s in enumerate(expected_quad_slices):
        np.testing.assert_allclose(mosaiced_image.data[s], extension_data[j])
        np.testing.assert_allclose(mosaiced_image.mask[s], extension_masks[j])



