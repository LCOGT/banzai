import numpy as np

from banzai.tests.utils import FakeImage
from banzai.crosstalk import CrosstalkCorrector


def test_no_input_images():
    tester = CrosstalkCorrector(None)
    images = tester.do_stage([])
    assert len(images) == 0


def test_crosstalk():
    tester = CrosstalkCorrector(None)
    nx = 101
    ny = 103

    images = [FakeImage(nx=nx, ny=ny) for x in range(6)]
    # Add random pixels at 10000 to each of the extensions
    for image in images:
        for amp in range(4):
            image.data = np.ones((4, ny, nx)) * 1000.0
            random_pixels_x = np.random.randint(0, nx - 1, size=int(0.05 * nx * ny))
            random_pixels_y = np.random.randint(0, ny - 1, size=int(0.05 * nx * ny))
            for i in zip(random_pixels_y, random_pixels_x):
                image.data[amp][i] = 10000

    expected_image_data = [image.data.copy() for image in images]

    # Simulate crosstalk
    for image in images:
        original_data = image.data.copy()
        for j in range(4):
            for i in range(4):
                if i != j:
                    crosstalk_coeff = np.random.uniform(0.0, 0.01)
                    image.header['CRSTLK{i}{j}'.format(i=i+1, j=j+1)] = crosstalk_coeff
                    image.data[j] += original_data[i] * crosstalk_coeff
    # Try to remove it
    images = tester.do_stage(images)
    # Assert that we got back the original image
    for i, image in enumerate(images):
        np.testing.assert_allclose(image.data, expected_image_data[i], atol=2.0, rtol=1e-5)
