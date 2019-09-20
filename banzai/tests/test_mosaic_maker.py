import pytest
import numpy as np

from banzai.mosaic import MosaicCreator
from banzai.tests.utils import FakeImage


class FakeMosaicImage(FakeImage):
    def __init__(self, *args, **kwargs):
        super(FakeMosaicImage, self).__init__(*args, **kwargs)
        self.ccdsum = '1 1'
        self.extension_headers = [{'DATASEC': '[1:512,1:512]', 'DETSEC': '[1025:2048,3072:2049]', 'CCDSUM': '2 2'},
                                  {'DATASEC': '[1:512,1:512]', 'DETSEC': '[1025:2048,1025:2048]', 'CCDSUM': '2 2'},
                                  {'DATASEC': '[1:512,1:512]', 'DETSEC': '[3072:2049,1025:2048]', 'CCDSUM': '2 2'},
                                  {'DATASEC': '[1:512,1:512]', 'DETSEC': '[3072:2049,3072:2049]', 'CCDSUM': '2 2'}]

    def update_shape(self, nx, ny):
        pass


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(200)


def test_get_mosaic_size():
    detsecs = [['[1:100,1:100]', '[1:100,200:101]', '[200:101,1:100]', '[200:101,200:101]'],
               ['[1:200,400:201]', '[1:200,1:200]', '[400:201,400:201]', '[400:201,1:200]'],
               ['[600:301,600:301]', '[600:301,1:300]', '[1:300,1:300]', '[1:300,600:301]'],
               ['[800:401,1:400]', '[800:401,800:401]', '[1:400,800:401]', '[1:400,1:400]'],
               ['[800:401,1:400]', None, '[1:400,800:401]', '[1:400,1:400]'],
               [None, None, None, None]]
    expected_mosaic_sizes = [(200, 200), (400, 400), (600, 600), (800, 800), (800, 800), (1, 1)]

    for i, detsec in enumerate(detsecs):
        fake_image = FakeMosaicImage()
        fake_image.extension_headers = [{'DETSEC': d} for d in detsec]
        assert expected_mosaic_sizes[i] == get_mosaic_size(fake_image, 4)


def test_null_input_image():
    mosaic_creator = MosaicCreator(None)
    image = mosaic_creator.run(None)
    assert image is None


def test_mosaic_size():
    image = FakeMosaicImage()
    image.ccdsum = '2 2'
    assert get_mosaic_size(image, 4) == (1024, 1024)


binned_windowed_extension_headers = [{'DETSEC': '[1025:2048,3072:2049]', 'CCDSUM': '2 2', 'DATASEC': '[1:512,1:512]'},
                                     {'DETSEC': '[1025:2048,1025:2048]', 'CCDSUM': '2 2', 'DATASEC': '[1:512,1:512]'},
                                     {'DETSEC': '[3072:2049,1025:2048]', 'CCDSUM': '2 2', 'DATASEC': '[1:512,1:512]'},
                                     {'DETSEC': '[3072:2049,3072:2049]', 'CCDSUM': '2 2', 'DATASEC': '[1:512,1:512]'}]


def test_mosaic_size_for_binned_windowed_mode():
    image = FakeMosaicImage()
    image.extension_headers = binned_windowed_extension_headers
    image.ccdsum = '2 2'
    assert get_mosaic_size(image, 4) == (1024, 1024)


def test_mosaic_maker_for_binned_windowed_mode():
    data_size = (4, 512, 512)
    expected_quad_slices = [(slice(1023, 511, -1), slice(0, 512)), (slice(0, 512), slice(0, 512)),
                            (slice(0, 512), slice(1023, 511, -1)), (slice(1023, 511, -1), slice(1023, 511, -1))]
    data = np.random.uniform(0, 1, size=data_size)
    bpm = np.random.choice([0, 1], size=data_size)
    image = FakeMosaicImage()
    image.ccdsum = '2 2'
    image.ny, image.nx = 512, 512
    image.data = data.copy()
    image.bpm = bpm.copy()
    image.extension_headers = binned_windowed_extension_headers

    mosaic_creator = MosaicCreator(None)
    mosaiced_image = mosaic_creator.do_stage(image)

    assert mosaiced_image.data.shape == (1024, 1024)
    for j, s in enumerate(expected_quad_slices):
        np.testing.assert_allclose(mosaiced_image.data[s], data[j])
        np.testing.assert_allclose(mosaiced_image.bpm[s], bpm[j])


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

    for i, detsec in enumerate(detsecs):
        data = np.random.uniform(0, 1, size=data_sizes[i])
        data_arrays.append(data)
        bpm = np.random.choice([0, 1], size=data_sizes[i])
        bpm_arrays.append(bpm)
        image = FakeMosaicImage()
        image.ny, image.nx = data_sizes[i][1:]
        image.data = data.copy()
        image.bpm = bpm.copy()
        image.extension_headers = []
        for j in range(4):
            image.extension_headers.append({'DATASEC': datasecs[i], 'DETSEC': detsec[j]})
        fake_images.append(image)

    mosaic_creator = MosaicCreator(None)
    mosaiced_images = [mosaic_creator.do_stage(fake_image) for fake_image in fake_images]

    for i, image in enumerate(mosaiced_images):
        assert image.data.shape == expected_mosaic_sizes[i]
        for j, s in enumerate(expected_quad_slices[i]):
            np.testing.assert_allclose(image.data[s], data_arrays[i][j])
            np.testing.assert_allclose(image.bpm[s], bpm_arrays[i][j])
