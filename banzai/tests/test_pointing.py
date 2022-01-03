import numpy as np
import pytest
from astropy.io.fits import Header

from banzai.qc.pointing import PointingTest
from banzai.tests.utils import FakeLCOObservationFrame, FakeCCDData

pytestmark = pytest.mark.pointing


def test_null_input_image():
    tester = PointingTest(None)
    image = tester.run(None)
    assert image is None


def test_no_offset():
    tester = PointingTest(None)

    image_header = Header({'CRVAL1': '1.0',
                           'CRVAL2': '-1.0',
                           'OFST-RA': '0:04:00.00',
                           'OFST-DEC': '-01:00:00.000'})

    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta=image_header)])
    image = tester.do_stage(image)

    np.testing.assert_allclose(image.meta.get('PNTOFST'), 0.0, atol=1e-7)


def test_large_offset():
    tester = PointingTest(None)

    image_header = Header({'CRVAL1': '00:00:00.000',
                           'CRVAL2': '-00:00:00.000',
                           'OFST-RA': '00:00:00.000',
                           'OFST-DEC': '-00:00:10.000'})

    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta=image_header)])
    image = tester.do_stage(image)

    np.testing.assert_allclose(image.meta.get('PNTOFST'), 10.0)
