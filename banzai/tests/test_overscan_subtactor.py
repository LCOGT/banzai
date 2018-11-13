import numpy as np

from banzai.bias import OverscanSubtractor
from banzai.tests.utils import FakeImage


class FakeOverscanImage(FakeImage):
    def __init__(self, *args, **kwargs):
        super(FakeOverscanImage, self).__init__(*args, **kwargs)
        self.header = {'BIASSEC': 'UNKNOWN'}


def test_no_input_images():
    subtractor = OverscanSubtractor(None)
    images = subtractor.do_stage([])
    assert len(images) == 0


def test_header_has_overscan_when_biassec_unknown():
    subtractor = OverscanSubtractor(None)
    images = subtractor.do_stage([FakeOverscanImage() for x in range(6)])
    for image in images:
        assert image.header['OVERSCAN'][0] == 0


def test_header_overscan_is_1():
    subtractor = OverscanSubtractor(None)
    nx = 101
    ny = 103
    noverscan = 10
    images = [FakeOverscanImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.header['BIASSEC'] = '[{nover}:{nx},1:{ny}]'.format(nover=noverscan, nx=nx, ny=ny)
    images = subtractor.do_stage(images)
    for image in images:
        assert image.header['OVERSCAN'][0] == 1


def test_header_overscan_is_2():
    subtractor = OverscanSubtractor(None)
    nx = 101
    ny = 103
    noverscan = 10
    images =[FakeOverscanImage(nx=nx, ny=ny, image_multiplier=2) for x in range(6)]
    for image in images:
        image.header['BIASSEC'] = '[{nover}:{nx},1:{ny}]'.format(nover=nx-noverscan, nx=nx, ny=ny)
    images = subtractor.do_stage(images)
    for image in images:
        assert image.header['OVERSCAN'][0] == 2


def test_overscan_estimation_is_reasonable():
    subtractor = OverscanSubtractor(None)
    nx = 101
    ny = 103
    noverscan = 10
    expected_read_noise = 10.0
    expected_overscan = 40.0
    input_level = 100

    images =[FakeOverscanImage(nx=nx, ny=ny, image_multiplier=input_level) for x in range(6)]
    for image in images:
        image.header['BIASSEC'] = '[{nover}:{nx},1:{ny}]'.format(nover=nx - noverscan + 1,
                                                                 nx=nx, ny=ny)
        image.data[:, -noverscan:] = np.random.normal(expected_overscan, expected_read_noise,
                                                      (ny, noverscan))
    images = subtractor.do_stage(images)
    for image in images:
        assert np.abs(image.header['OVERSCAN'][0] - expected_overscan) < 1.0
        assert np.abs(np.mean(image.data[:, :-noverscan]) - input_level + expected_overscan) < 1.0

# TODO: Add test for 2d overscan subtractor

# TODO: Add test for 3d overscan subtractor

# TODO: Copy 2d tests for 3d data cubes
