import numpy as np

from banzai.qc.pointing import PointingTest
from banzai.tests.utils import FakeImage


def test_null_input_image():
    tester = PointingTest(None)
    image = tester.run(None)
    assert image is None


def test_no_offset():
    tester = PointingTest(None)
    nx = 101
    ny = 103

    image = FakeImage(nx=nx, ny=ny)
    image.header['CRVAL1'] = '1.0'
    image.header['CRVAL2'] = '-1.0'
    image.header['OFST-RA'] = '0:04:00.00'
    image.header['OFST-DEC'] = '-01:00:00.000'

    image = tester.do_stage(image)
    np.testing.assert_allclose(image.header['PNTOFST'][0], 0.0, atol=1e-7)


def test_large_offset():
    tester = PointingTest(None)
    nx = 101
    ny = 103

    image = FakeImage(nx=nx, ny=ny)
    image.header['CRVAL1'] = '00:00:00.000'
    image.header['CRVAL2'] = '-00:00:00.000'
    image.header['OFST-RA'] = '00:00:00.000'
    image.header['OFST-DEC'] = '-00:00:10.000'

    image = tester.do_stage(image)
    assert image.header['PNTOFST'][0] == 10.0
