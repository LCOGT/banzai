import numpy as np

from banzai.mosaic import get_mosaic_size, MosaicCreator
from banzai.tests.utils import FakeImage


class FakeMosaicImage(FakeImage):
    def __init__(self, *args, **kwargs):
        super(FakeMosaicImage, self).__init__(*args, **kwargs)
        self.extension_headers = None

    def update_shape(self, nx, ny):
        pass


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


def test_no_input_images():
    mosaic_creator = MosaicCreator(None)
    images = mosaic_creator.do_stage([])
    assert len(images) == 0


def test_2d_images():
    pass


def test_missing_detsecs():
    pass


def test_missing_datasecs():
    pass


def test_mosaic_maker():
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
    mosaiced_images = mosaic_creator.do_stage(fake_images)

    for i, image in enumerate(mosaiced_images):
        assert image.data.shape == expected_mosaic_sizes[i]
        for j, s in enumerate(expected_quad_slices[i]):
            np.testing.assert_allclose(image.data[s], data_arrays[i][j])
            np.testing.assert_allclose(image.bpm[s], bpm_arrays[i][j])
