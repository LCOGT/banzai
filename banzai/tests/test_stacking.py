import numpy as np
import pytest

from banzai.images import stack
from banzai.tests.utils import FakeCCDData

pytestmark = pytest.mark.stacking


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(1019283)


def test_stacking(set_random_seed):
    nx, ny = 102, 105
    test_data = [FakeCCDData(data=np.ones((ny, nx)) * i,
                             mask=np.zeros((ny, nx)),
                             uncertainty=np.ones((ny, nx)) * 3.0) for i in range(9)]

    stacked_data = stack(test_data, 1e5)
    np.testing.assert_allclose(stacked_data.data, np.ones((ny, nx)) * np.mean(np.arange(9)))
    np.testing.assert_allclose(stacked_data.uncertainty, np.ones((ny, nx)))
    assert np.all(stacked_data.mask == 0)


def test_stacking_with_noise(set_random_seed):
    nx, ny = 102, 105
    test_data = [FakeCCDData(data=np.random.normal(0.0, 3.0, size=(ny, nx)),
                             mask=np.zeros((ny, nx), dtype=np.uint8),
                             uncertainty=np.ones((ny, nx)) * 3.0) for i in range(81)]

    stacked_data = stack(test_data, 1e5)
    np.testing.assert_allclose(stacked_data.data, np.zeros((ny, nx)), atol=5.0/3.0)
    np.testing.assert_allclose(stacked_data.uncertainty, np.ones((ny, nx)) / 3.0)
    assert np.all(stacked_data.mask == 0)


def test_stacking_with_different_pixels(set_random_seed):
    nx, ny = 102, 105
    d = np.arange(nx*ny, dtype=np.float).reshape(ny, nx)
    test_data = [FakeCCDData(data=d * i,
                             mask=np.zeros((ny, nx), dtype=np.uint8),
                             uncertainty=np.ones((ny, nx)) * 3.0) for i in range(9)]

    stacked_data = stack(test_data, 1e5)
    np.testing.assert_allclose(stacked_data.data, 4.0 * d)
    np.testing.assert_allclose(stacked_data.uncertainty, np.ones((ny, nx)))
    assert np.all(stacked_data.mask == 0)
