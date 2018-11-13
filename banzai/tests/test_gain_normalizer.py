import numpy as np

from banzai.gain import GainNormalizer, validate_gain
from banzai.tests.utils import FakeImage


class FakeGainImage(FakeImage):
    def __init__(self, *args, **kwargs):
        super(FakeGainImage, self).__init__(*args, **kwargs)
        self.gain = None


def test_no_input_images():
    gain_normalizer = GainNormalizer(None)
    images = gain_normalizer.do_stage([])
    assert len(images) == 0


def test_gain_header_missing():
    gain_normalizer = GainNormalizer(None)
    images = gain_normalizer.do_stage([FakeGainImage() for x in range(6)])
    assert len(images) == 0


def test_gain_header_0():
    gain_normalizer = GainNormalizer(None)
    fake_images = [FakeGainImage() for x in range(6)]
    for image in fake_images:
        image.gain = 0.0
    images = gain_normalizer.do_stage(fake_images)
    assert len(images) == 0


def test_gain_is_empty_list():
    gain_normalizer = GainNormalizer(None)
    fake_images = [FakeGainImage() for x in range(6)]
    for image in fake_images:
        image.gain = []
    images = gain_normalizer.do_stage(fake_images)
    assert len(images) == 0


def test_gain_1d():
    nx, ny = 101, 103
    n_images = 6
    saturation = 65536
    max_linearity = 60000
    input_gains = np.random.uniform(0.5, 2.5, size=n_images)
    input_data = [np.random.normal(10, 1, size=(ny, nx)) for i in range(n_images)]

    fake_images = [FakeGainImage(nx=nx, ny=ny) for x in range(n_images)]

    for i, image in enumerate(fake_images):
        image.gain = input_gains[i]
        image.data = input_data[i].copy()
        image.header['SATURATE'] = saturation
        image.header['MAXLIN'] = max_linearity

    gain_normalizer = GainNormalizer(None)
    output_images = gain_normalizer.do_stage(fake_images)

    for i, image in enumerate(output_images):
        np.testing.assert_allclose(image.data, input_data[i] * input_gains[i])
        np.testing.assert_allclose(image.header['SATURATE'], saturation * input_gains[i])
        np.testing.assert_allclose(image.header['MAXLIN'], max_linearity * input_gains[i])


def test_gain_datacube():
    n_amplifiers = 4
    nx, ny = 101, 103
    n_images = 6
    saturation = 65536
    max_linearity = 60000
    # These tests will fail if the gain is a numpy array because it will try to check element by
    # element which raises and exception here.
    input_gains = [list(np.random.uniform(0.5, 2.5, size=n_amplifiers)) for i in range(n_images)]
    input_data = [np.random.normal(10, 1, size=(n_amplifiers, ny, nx)) for i in range(n_images)]

    fake_images = [FakeGainImage(nx=nx, ny=ny) for i in range(n_images)]

    for i, image in enumerate(fake_images):
        image.gain = input_gains[i]
        image.data = input_data[i].copy()
        image.header['SATURATE'] = saturation
        image.header['MAXLIN'] = max_linearity

    gain_normalizer = GainNormalizer(None)
    output_images = gain_normalizer.do_stage(fake_images)

    for i, image in enumerate(output_images):
        for j in range(n_amplifiers):
            np.testing.assert_allclose(image.data[j], input_data[i][j] * input_gains[i][j])
        np.testing.assert_allclose(image.header['SATURATE'], saturation * min(input_gains[i]))
        np.testing.assert_allclose(image.header['MAXLIN'], max_linearity * min(input_gains[i]))


def test_gain_missing():
    assert validate_gain([])
    assert validate_gain(None)
    assert validate_gain(0.0)
    assert not validate_gain(1.0)
    assert not validate_gain([1.0, 2.0])
    assert validate_gain([1.0, 0.0, 2.0])
