import pytest
import numpy as np

from banzai.gain import GainNormalizer, validate_gain
from banzai.tests.utils import FakeImage


class FakeGainImage(FakeImage):
    def __init__(self, *args, **kwargs):
        super(FakeGainImage, self).__init__(*args, **kwargs)
        self.gain = None


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(200)


def test_null_input_image():
    gain_normalizer = GainNormalizer(None)
    image = gain_normalizer.run(None)
    assert image is None


def test_gain_header_missing():
    gain_normalizer = GainNormalizer(None)
    image = gain_normalizer.do_stage(FakeGainImage())
    assert image is None


def test_gain_header_0():
    gain_normalizer = GainNormalizer(None)
    image = FakeGainImage()
    image.gain = 0.0
    image = gain_normalizer.do_stage(image)
    assert image is None


def test_gain_is_empty_list():
    gain_normalizer = GainNormalizer(None)
    image = FakeGainImage()
    image.gain = []
    image = gain_normalizer.do_stage(image)
    assert image is None


def test_gain_1d(set_random_seed):
    nx, ny = 101, 103
    saturation = 65536
    max_linearity = 60000
    input_gains = np.random.uniform(0.5, 2.5)
    input_data = np.random.normal(10, 1, size=(ny, nx))

    image = FakeGainImage(nx=nx, ny=ny)

    image.gain = input_gains
    image.data = input_data.copy()
    image.header['SATURATE'] = saturation
    image.header['MAXLIN'] = max_linearity

    gain_normalizer = GainNormalizer(None)
    image = gain_normalizer.do_stage(image)

    np.testing.assert_allclose(image.data, input_data * input_gains)
    np.testing.assert_allclose(image.header['SATURATE'], saturation * input_gains)
    np.testing.assert_allclose(image.header['MAXLIN'], max_linearity * input_gains)


def test_gain_datacube(set_random_seed):
    n_amplifiers = 4
    nx, ny = 101, 103
    saturation = 65536
    max_linearity = 60000
    # These tests will fail if the gain is a numpy array because it will try to check element by
    # element which raises and exception here.
    input_gains = list(np.random.uniform(0.5, 2.5, size=n_amplifiers))
    input_data = np.random.normal(10, 1, size=(n_amplifiers, ny, nx))

    image = FakeGainImage(nx=nx, ny=ny)

    image.gain = input_gains
    image.data = input_data.copy()
    image.header['SATURATE'] = saturation
    image.header['MAXLIN'] = max_linearity

    gain_normalizer = GainNormalizer(None)
    image = gain_normalizer.do_stage(image)

    for i in range(n_amplifiers):
        np.testing.assert_allclose(image.data[i], input_data[i] * input_gains[i])
    np.testing.assert_allclose(image.header['SATURATE'], saturation * min(input_gains))
    np.testing.assert_allclose(image.header['MAXLIN'], max_linearity * min(input_gains))


def test_gain_missing():
    assert validate_gain([])
    assert validate_gain(None)
    assert validate_gain(0.0)
    assert not validate_gain(1.0)
    assert not validate_gain([1.0, 2.0])
    assert validate_gain([1.0, 0.0, 2.0])
