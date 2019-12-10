from banzai.images import stack
import numpy as np
import pytest


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(1019283)


class test_ccddata:
    def __init__(self, data, mask, uncertainty):
        self.data = data
        self.mask = mask
        self.uncertainty = uncertainty
        self.meta = {}

    @property
    def shape(self):
        return self.data.shape

    @property
    def dtype(self):
        return self.data.dtype


def test_stacking():
    nx, ny = 102, 105
    test_data = [test_ccddata(np.ones((ny, nx)) * i, np.zeros((ny, nx), dtype=np.uint8), np.ones((ny, nx)) * 3.0)
                 for i in range(9)]
    stacked_data = stack(test_data, 1e5)
    np.testing.assert_allclose(stacked_data.data, np.ones((ny, nx)) * np.mean(np.arange(9)))
    np.testing.assert_allclose(stacked_data.uncertainty, np.ones((ny, nx)))
    assert np.all(stacked_data.mask == 0)


def test_stacking_with_noise():
    nx, ny = 102, 105
    test_data = [test_ccddata(np.random.normal(0.0, 3.0, size=(ny, nx)),
                              np.zeros((ny, nx), dtype=np.uint8),
                              np.ones((ny, nx)) * 3.0)
                 for i in range(81)]
    stacked_data = stack(test_data, 1e5)
    np.testing.assert_allclose(stacked_data.data, np.zeros((ny, nx)), atol=5.0/3.0)
    np.testing.assert_allclose(stacked_data.uncertainty, np.ones((ny, nx)) / 3.0)
    assert np.all(stacked_data.mask == 0)


def test_stacking_with_different_pixels():
    nx, ny = 102, 105
    d = np.arange(nx*ny, dtype=np.float).reshape(ny, nx)
    test_data = [test_ccddata(d * i,
                              np.zeros((ny, nx), dtype=np.uint8),
                              np.ones((ny, nx)) * 3.0)
                 for i in range(9)]
    stacked_data = stack(test_data, 1e5)
    np.testing.assert_allclose(stacked_data.data, 4.0 * d)
    np.testing.assert_allclose(stacked_data.uncertainty, np.ones((ny, nx)))
    assert np.all(stacked_data.mask == 0)
