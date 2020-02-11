import pytest
import numpy as np
from astropy.io.fits import Header

from banzai.gain import GainNormalizer
from banzai.tests.utils import FakeLCOObservationFrame, FakeCCDData, FakeContext

pytestmark = pytest.mark.gain_normalizer

test_header = Header({'SATURATE': 35000,
                      'GAIN': 3.54,
                      'MAXLIN': 35000})

@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(200)


def test_null_input_image():
    gain_normalizer = GainNormalizer(FakeContext())
    image = gain_normalizer.run(None)
    assert image is None


def test_gain_header_0():
    gain_normalizer = GainNormalizer(FakeContext())
    image = gain_normalizer.do_stage(FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta=test_header, gain=0.0)]))
    for hdu in image.ccd_hdus:
        assert (hdu.data == np.zeros(hdu.data.shape)).all()


def test_gain_1d(set_random_seed):
    nx, ny = 101, 103
    saturation = 65536
    max_linearity = 60000
    input_gain = np.random.uniform(0.5, 2.5)
    input_data = np.random.normal(10, 1, size=(ny, nx))

    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=input_data.copy(),
                                                          gain=input_gain,
                                                          meta={'SATURATE': saturation,
                                                                'MAXLIN': max_linearity})])

    gain_normalizer = GainNormalizer(FakeContext())
    image = gain_normalizer.do_stage(image)

    np.testing.assert_allclose(image.data, input_data * input_gain)
    np.testing.assert_allclose(image.meta['SATURATE'], saturation * input_gain)
    np.testing.assert_allclose(image.meta['MAXLIN'], max_linearity * input_gain)


def test_gain_datacube(set_random_seed):
    n_amplifiers = 4
    nx, ny = 101, 103
    saturation = 65536
    max_linearity = 60000

    input_gains = list(np.random.uniform(0.5, 2.5, size=n_amplifiers))
    input_data = np.random.normal(10, 1, size=(n_amplifiers, ny, nx))

    hdu_list = []
    for gain, data in zip(input_gains, input_data):
        hdu_list.append(FakeCCDData(data=data.copy(), gain=gain, meta={'SATURATE': saturation,
                                                                       'MAXLIN': max_linearity}))

    image = FakeLCOObservationFrame(hdu_list=hdu_list)

    gain_normalizer = GainNormalizer(None)
    image = gain_normalizer.do_stage(image)

    for i in range(n_amplifiers):
        np.testing.assert_allclose(image.ccd_hdus[i].data, input_data[i] * input_gains[i])
    np.testing.assert_allclose(image.meta['SATURATE'], saturation * min(input_gains))
    np.testing.assert_allclose(image.meta['MAXLIN'], max_linearity * min(input_gains))
