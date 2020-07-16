import pytest
import numpy as np

from banzai.dark import DarkNormalizer
from banzai.tests.utils import FakeCCDData, FakeLCOObservationFrame, FakeContext

pytestmark = pytest.mark.dark_normalizer


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(7298374)


def test_null_input_images():
    normalizer = DarkNormalizer(None)
    image = normalizer.run(None)
    assert image is None


def test_dark_normalization_is_reasonable(set_random_seed):
    nx = 101
    ny = 103
    exposure_time = 900.0
    saturation_level = 35000
    read_noise = 10.0
    gain = 3.54

    normalizer = DarkNormalizer(FakeContext())
    data = np.random.normal(30.0, 10, size=(ny, nx))
    uncertainty = np.random.normal(0.0, read_noise, size=(ny, nx))

    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=data.copy(),
                                                          uncertainty=uncertainty.copy(),
                                                          meta={'EXPTIME': exposure_time,
                                                                'SATURATE': saturation_level,
                                                                'GAIN': gain,
                                                                'MAXLIN': saturation_level})])

    image = normalizer.do_stage(image)

    np.testing.assert_allclose(image.data, data / image.exptime, 1e-5)
    np.testing.assert_allclose(image.primary_hdu.uncertainty, uncertainty / image.exptime, 1e-5)
    assert image.meta['SATURATE'] == saturation_level / image.exptime
    assert image.meta['MAXLIN'] == saturation_level / image.exptime
    assert image.meta['GAIN'] == gain * image.exptime
