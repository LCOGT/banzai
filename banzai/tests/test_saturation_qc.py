from .utils import FakeImage
from banzai.qc import SaturationTest


def test_no_input_images():
    tester = SaturationTest(None)
    images = tester.do_stage([])
    assert len(images) == 0


def test_group_by_keywords():
    tester = SaturationTest(None)
    assert tester.group_by_keywords is None


def test_no_pixels_saturated():
    tester = SaturationTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.header['SATURATE'] = 65535

    images = tester.do_stage(images)
    for image in images:
        assert image.header['SATFRAC'][0] == 0.0
    assert len(images) == 6


def test_nonzero_but_no_pixels_saturated():
    tester = SaturationTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.header['SATURATE'] = 65535
        image.data += 5.0

    images = tester.do_stage(images)
    for image in images:
        assert image.header['SATFRAC'][0] == 0.0
    assert len(images) == 6

