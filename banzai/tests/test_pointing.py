import numpy as np

from .utils import FakeImage
from banzai.pointing import PointingTest


def test_no_input_images():
    tester = PointingTest(None)
    images = tester.do_stage([])
    assert len(images) == 0


def test_group_by_keywords():
    tester = PointingTest(None)
    assert tester.group_by_keywords is None


def test_no_offset():
    tester = PointingTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.header['CRVAL1'] = '1.0'
        image.header['CRVAL2'] = '-1.0'
        image.header['OFST-RA'] = '0:04:00.00'
        image.header['OFST-DEC'] = '-01:00:00.000'

    images = tester.do_stage(images)
    for image in images:
        np.testing.assert_allclose(image.header['PNTOFST'][0], 0.0, atol=1e-7)


def test_large_offset():
    tester = PointingTest(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    for image in images:
        image.header['CRVAL1'] = '00:00:00.000'
        image.header['CRVAL2'] = '-00:00:00.000'
        image.header['OFST-RA'] = '00:00:00.000'
        image.header['OFST-DEC'] = '-00:00:10.000'

    images = tester.do_stage(images)
    for image in images:
        assert image.header['PNTOFST'][0] == 10.0
