import pytest
import numpy as np
import pytest
from astropy.io.fits import Header

from banzai.bias import OverscanSubtractor
from banzai.tests.utils import FakeLCOObservationFrame, FakeCCDData

pytestmark = pytest.mark.overscan_subtractor


class FakeOverscanImage(FakeLCOObservationFrame):
    def __init__(self, *args, **kwargs):
        super(FakeOverscanImage, self).__init__(*args, **kwargs)
        self.primary_hdu.meta = Header({'BIASSEC': 'UNKNOWN'})


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(200)


def test_null_input_image():
    subtractor = OverscanSubtractor(None)
    image = subtractor.run(None)
    assert image is None


def test_header_has_overscan_when_biassec_unknown():
    subtractor = OverscanSubtractor(None)
    image = subtractor.do_stage(FakeOverscanImage())
    assert image.meta['OVERSCAN'] == 0


def test_header_overscan_is_1():
    subtractor = OverscanSubtractor(None)
    nx = 101
    ny = 103
    noverscan = 10
    image = FakeOverscanImage()
    image.primary_hdu.meta['BIASSEC'] = '[{nover}:{nx},1:{ny}]'.format(nover=noverscan, nx=nx, ny=ny)
    image = subtractor.do_stage(image)
    assert image.meta['OVERSCAN'] == 1


def test_header_overscan_is_2():
    subtractor = OverscanSubtractor(None)
    nx = 101
    ny = 103
    noverscan = 10
    image = FakeOverscanImage(hdu_list=[FakeCCDData(image_multiplier=2)])
    image.meta['BIASSEC'] = '[{nover}:{nx},1:{ny}]'.format(nover=nx-noverscan, nx=nx, ny=ny)
    image = subtractor.do_stage(image)
    assert image.meta['OVERSCAN'] == 2


def test_overscan_subtractor_3d():
    subtractor = OverscanSubtractor(None)
    nx = 101
    ny = 103
    noverscan = 10
    image_multipliers = range(4)
    image = FakeOverscanImage(hdu_list=[FakeCCDData(image_multiplier=multiplier,
                                                    meta=Header({'BIASSEC': '[{nover}:{nx},1:{ny}]'.format(nover=nx-noverscan, nx=nx, ny=ny)}))
                                        for multiplier in image_multipliers])
    image = subtractor.do_stage(image)
    for i, multiplier in enumerate(image_multipliers):
        assert image.ccd_hdus[i].meta['OVERSCAN'] == multiplier


def test_overscan_estimation_is_reasonable(set_random_seed):
    subtractor = OverscanSubtractor(None)
    nx = 101
    ny = 103
    noverscan = 10
    expected_read_noise = 10.0
    expected_overscan = 40.0
    input_level = 100

    image = FakeOverscanImage(hdu_list=[FakeCCDData(image_multiplier=input_level)])
    image.meta['BIASSEC'] = '[{nover}:{nx},1:{ny}]'.format(nover=nx - noverscan + 1,
                                                             nx=nx, ny=ny)
    image.data[:, -noverscan:] = np.random.normal(expected_overscan, expected_read_noise,
                                                  (ny, noverscan))
    image = subtractor.do_stage(image)
    assert np.abs(image.meta['OVERSCAN'] - expected_overscan) < 1.0
    assert np.abs(np.mean(image.data[:, :-noverscan]) - input_level + expected_overscan) < 1.0
