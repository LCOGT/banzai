import pytest
import numpy as np

from banzai.tests.utils import FakeImage
from banzai.crosstalk import CrosstalkCorrector


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

    image = FakeImage(nx=nx, ny=ny)
    # Add random pixels at 10000 to each of the extensions
    for amp in range(4):
        image.data = np.ones((4, ny, nx)) * 1000.0
        random_pixels_x = np.random.randint(0, nx - 1, size=int(0.05 * nx * ny))
        random_pixels_y = np.random.randint(0, ny - 1, size=int(0.05 * nx * ny))
        for i in zip(random_pixels_y, random_pixels_x):
            image.data[amp][i] = 10000

    expected_image_data = image.data.copy()

    # Simulate crosstalk
    original_data = image.data.copy()
    for j in range(4):
        for i in range(4):
            if i != j:
                crosstalk_coeff = np.random.uniform(0.0, 0.01)
                image.header['CRSTLK{i}{j}'.format(i=i+1, j=j+1)] = crosstalk_coeff
                image.data[j] += original_data[i] * crosstalk_coeff
    # Try to remove it
    image = tester.do_stage(image)
    # Assert that we got back the original image
    np.testing.assert_allclose(image.data, expected_image_data, atol=2.0, rtol=1e-5)
