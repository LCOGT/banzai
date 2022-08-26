import pytest
import numpy as np

from banzai.flats import FlatSNRChecker
from banzai.tests.utils import FakeContext, FakeCCDData, FakeLCOObservationFrame

pytestmark = pytest.mark.flat_snr


def test_rejects_high_noise():
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.ones((100,100)),
                                                          uncertainty=1000*np.ones((100, 100)))])

    snr_checker = FlatSNRChecker(FakeContext())
    image = snr_checker.do_stage(image)

    assert image is None
