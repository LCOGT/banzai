import pytest
import numpy as np
import copy

from banzai.tests.utils import FakeLCOObservationFrame, FakeCCDData
from banzai.crosstalk import CrosstalkCorrector

pytestmark = pytest.mark.crosstalk_corrector


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(6234585)


def test_null_input_image():
    tester = CrosstalkCorrector(None)
    image = tester.run(None)
    assert image is None


def test_crosstalk(set_random_seed):
    tester = CrosstalkCorrector(None)
    nx = 101
    ny = 103

    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(nx=nx, ny=ny) for amp in range(4)], data_is_3d=True)
    # Add random pixels at 10000 to each of the extensions
    for extension in image.ccd_hdus:
        extension.data = np.ones((ny, nx)) * 1000.0
        random_pixels_x = np.random.randint(0, nx - 1, size=int(0.05 * nx * ny))
        random_pixels_y = np.random.randint(0, ny - 1, size=int(0.05 * nx * ny))
        for i in zip(random_pixels_y, random_pixels_x):
            extension.data[i] = 10000

    expected_image_data = copy.copy(image)

    # Simulate crosstalk
    original_data = copy.copy(image)
    for j in range(4):
        for i in range(4):
            if i != j:
                crosstalk_coeff = np.random.uniform(0.0, 0.01)
                image.meta['CRSTLK{i}{j}'.format(i=i+1, j=j+1)] = crosstalk_coeff
                image.ccd_hdus[j].data += original_data.ccd_hdus[i].data * crosstalk_coeff
    # Try to remove it
    image = tester.do_stage(image)
    # Assert that we got back the original image
    for image_hdu, expected_image_hdu in zip(image.ccd_hdus, expected_image_data.ccd_hdus):
        np.testing.assert_allclose(image_hdu.data, expected_image_hdu.data, atol=2.0, rtol=1e-5)
