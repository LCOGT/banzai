import numpy as np

from banzai.utils import median_utils


def test_quick_select_arange():
    for i in range(20):
        size = np.random.randint(2, 10000)
        index = np.random.randint(0, size - 1)
        expected = index
        a = np.arange(size, dtype=np.float32)
        np.random.shuffle(a)
        assert median_utils._quick_select(a, index) == np.float32(expected)


def _compare_quick_select(a, index):
    actual = median_utils._quick_select(a.astype('f4'), index)
    a = a.astype('f4')
    a.sort()
    expected = a[index]
    assert actual == np.float32(expected)


def test_quick_select_normal_distribution():
    for i in range(20):
        mean = np.random.uniform(-1000.0, 1000.0)
        sigma = np.random.uniform(0, 100.0)
        size = np.random.randint(2, 10000)
        index = np.random.randint(0, size - 1)
        a = np.random.normal(mean, sigma, size=size)
        _compare_quick_select(a, index)


def test_quick_select_uniform_distribution():
    for i in range(20):
        size = np.random.randint(2, 10000)
        index = np.random.randint(0, size - 2)
        a = np.random.uniform(-1000.0, 1000.0, size=size)
        _compare_quick_select(a, index)


def test_quick_select_bimodel_arange():
    for i in range(20):
        size1 = np.random.randint(2, 10000)
        size2 = np.random.randint(2, 10000)
        start1 = np.random.randint(0, 10000)
        start2 = np.random.randint(0, 10000)
        index = np.random.randint(0, size1 + size2 - 1)
        a = np.append(np.arange(size1, dtype=np.float32) + start1,
                      np.arange(size2, dtype=np.float32) + start2)
        np.random.shuffle(a)
        _compare_quick_select(a, index)


def test_quick_select_bimodal_normal_distribution():
    for i in range(20):
        mean1 = np.random.uniform(-1000.0, 1000.0)
        sigma1 = np.random.uniform(0, 100.0)
        mean2 = np.random.uniform(-1000.0, 1000.0)
        sigma2 = np.random.uniform(0, 100.0)
        size1 = np.random.randint(2, 10000)
        size2 = np.random.randint(2, 10000)
        index = np.random.randint(0, size1 + size2 - 1)
        a = np.append(np.random.normal(mean1, sigma1, size=size1),
                      np.random.normal(mean2, sigma2, size=size2))
        _compare_quick_select(a, index)


def test_quick_select_bimodal_uniformal_distribution():
    for i in range(20):
        size1 = np.random.randint(2, 10000)
        size2 = np.random.randint(2, 10000)
        center1 = np.random.uniform(-10000.0, 10000.0)
        center2 = np.random.uniform(-10000.0, 10000.0)
        index = np.random.randint(0, size1 + size2 - 1)
        a = np.append(np.random.normal(-1000.0, 1000.0, size=size1) + center1,
                      np.random.normal(-1000.0, 1000.0, size=size2) + center2)
        _compare_quick_select(a, index)
