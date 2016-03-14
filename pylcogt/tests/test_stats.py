from ..utils import stats
import numpy as np


def test_quick_select_arange():
    for i in range(100):
        size = np.random.choice(10000)
        index = np.random.choice(size - 1)
        expected = index
        a = np.arange(size, dtype=np.float32)
        np.random.shuffle(a)
        assert stats._quick_select(a, index) == np.float32(expected)


def _compare_quick_select(a, index):
    actual = stats._quick_select(a.astype('f4'), index)
    a.sort()
    expected = a[index]
    assert actual == np.float32(expected)


def test_quick_select_normal_distribution():
    for i in range(100):
        mean = np.random.uniform(-1000.0, 1000.0)
        sigma = np.random.uniform(0, 100.0)
        size = np.random.choice(10000)
        index = np.random.choice(size - 1)
        a = np.random.normal(mean, sigma, size=size)
        _compare_quick_select(a, index)


def test_quick_select_normal_uniform():
    for i in range(100):
        size = np.random.choice(10000)
        index = np.random.choice(size - 1)
        a = np.random.uniform(-1000.0, 1000.0, size=size)
        _compare_quick_select(a, index)


def test_quick_select_bimodel_arange():
    for i in range(100):
        size1 = np.random.choice(10000)
        size2 = np.random.choice(10000)
        start1 = np.random.choice(10000)
        start2 = np.random.choice(10000)
        index = np.random.choice(size1 + size2 - 1)
        a = np.append(np.arange(size1, dtype=np.float32) + start1,
                      np.arange(size2, dtype=np.float32) + start2)
        _compare_quick_select(a, index)


def test_quick_select_bimodal_normal_distribution():
    for i in range(100):
        mean1 = np.random.uniform(-1000.0, 1000.0)
        sigma1 = np.random.uniform(0, 100.0)
        mean2 = np.random.uniform(-1000.0, 1000.0)
        sigma2 = np.random.uniform(0, 100.0)
        size1 = np.random.choice(10000)
        size2 = np.random.choice(10000)
        index = np.random.choice(size1 + size2 - 1)
        a = np.append(np.random.normal(mean1, sigma1, size=size1),
                      np.random.normal(mean2, sigma2, size=size2))
        _compare_quick_select(a, index)


def test_quick_select_bimodal_uniformal_distribution():
    for i in range(100):
        size1 = np.random.choice(10000)
        size2 = np.random.choice(10000)
        center1 = np.random.uniform(-10000.0, 10000.0)
        center2 = np.random.uniform(-10000.0, 10000.0)
        index = np.random.choice(size1 + size2 - 1)
        a = np.append(np.random.normal(-1000.0, 1000.0, size=size1) + center1,
                      np.random.normal(-1000.0, 1000.0, size=size2) + center2)
        _compare_quick_select(a, index)
