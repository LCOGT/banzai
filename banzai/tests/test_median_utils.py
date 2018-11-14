import numpy as np

from banzai.utils import median_utils


def _compare_median1d(a, mask):
    a_copy = a.copy()
    actual = median_utils.median1d(a.astype('f4'), mask=mask.astype(np.uint8))
    expected = np.median(a_copy[mask == 0].astype('f4'))
    assert actual == np.float32(expected)


def test_median1d_arange_nomask():
    for i in range(20):
        size = np.random.choice(10000)
        a = np.arange(size, dtype=np.float32)
        np.random.shuffle(a)
        _compare_median1d(a, np.zeros(size, dtype=np.uint8))


def test_median1d_normal_distribution_nomask():
    for i in range(20):
        mean = np.random.uniform(-1000.0, 1000.0)
        sigma = np.random.uniform(0, 100.0)
        size = np.random.choice(10000)
        a = np.random.normal(mean, sigma, size=size)
        _compare_median1d(a.astype('f4'), np.zeros(size, dtype=np.uint8))


def test_median1d_uniform_distribution_nomask():
    for i in range(20):
        size = np.random.choice(10000)
        a = np.random.uniform(-1000.0, 1000.0, size=size)
        _compare_median1d(a.astype('f4'), np.zeros(size, dtype=np.uint8))


def test_median1d_bimodel_arange_nomask():
    for i in range(20):
        size1 = np.random.randint(1, 10000)
        size2 = np.random.randint(1, 10000)
        start1 = np.random.randint(0, 10000)
        start2 = np.random.randint(0, 10000)
        a = np.append(np.arange(size1, dtype=np.float32) + start1,
                      np.arange(size2, dtype=np.float32) + start2)
        _compare_median1d(a, np.zeros(size1 + size2, dtype=np.uint8))


def test_median1d_bimodal_normal_distribution_nomask():
    for i in range(20):
        mean1 = np.random.uniform(-1000.0, 1000.0)
        sigma1 = np.random.uniform(0, 100.0)
        mean2 = np.random.uniform(-1000.0, 1000.0)
        sigma2 = np.random.uniform(0, 100.0)
        size1 = np.random.randint(1, 10000)
        size2 = np.random.randint(1, 10000)
        a = np.append(np.random.normal(mean1, sigma1, size=size1),
                      np.random.normal(mean2, sigma2, size=size2))
        _compare_median1d(a, np.zeros(size1 + size2, dtype=np.uint8))


def test_median1d_bimodal_uniformal_distribution_nomask():
    for i in range(20):
        size1 = np.random.randint(1, 10000)
        size2 = np.random.randint(1, 10000)
        center1 = np.random.uniform(-10000.0, 10000.0)
        center2 = np.random.uniform(-10000.0, 10000.0)
        a = np.append(np.random.normal(-1000.0, 1000.0, size=size1) + center1,
                      np.random.normal(-1000.0, 1000.0, size=size2) + center2)
        _compare_median1d(a, np.zeros(size1 + size2, dtype=np.uint8))


def test_median1d_all_masks_returns_zero():
    size = 1000
    a = np.zeros(size, dtype=np.float32)
    mask = np.ones(size, dtype=np.uint8)
    assert median_utils.median1d(a, mask) == 0.0


def test_median1d_arange_mask():
    for i in range(20):
        size = np.random.randint(1, 10000)
        a = np.arange(size, dtype=np.float32)
        np.random.shuffle(a)
        mask = np.zeros(size, dtype=np.uint8)
        mask[np.random.randint(0, size - 1, size=np.random.randint(1, size - 1))] = 1
        _compare_median1d(a, mask)


def test_median1d_normal_distribution_mask():
    for i in range(20):
        mean = np.random.uniform(-1000.0, 1000.0)
        sigma = np.random.uniform(0, 100.0)
        size = np.random.randint(1, 10000)
        a = np.random.normal(mean, sigma, size=size)
        mask = np.zeros(size, dtype=np.uint8)
        mask[np.random.randint(0, size - 1, size=np.random.randint(1, size - 1))] = 1
        _compare_median1d(a.astype('f4'), mask)


def test_median1d_uniform_distribution_mask():
    for i in range(20):
        size = np.random.randint(1, 10000)
        a = np.random.uniform(-1000.0, 1000.0, size=size)
        mask = np.zeros(size, dtype=np.uint8)
        mask[np.random.randint(0, size - 1, size=np.random.randint(1, size - 1))] = 1
        _compare_median1d(a.astype('f4'), mask)


def test_median1d_bimodel_arange_mask():
    for i in range(20):
        size1 = np.random.randint(1, 10000)
        size2 = np.random.randint(1, 10000)
        start1 = np.random.randint(0, 10000)
        start2 = np.random.randint(0, 10000)
        mask = np.zeros(size1 + size2, dtype=np.uint8)
        mask[np.random.randint(0, size1 + size2 - 1, size=np.random.randint(1, size1 + size2 - 1))] = 1
        a = np.append(np.arange(size1, dtype=np.float32) + start1,
                      np.arange(size2, dtype=np.float32) + start2)
        _compare_median1d(a, mask)


def test_median1d_bimodal_normal_distribution_mask():
    for i in range(20):
        mean1 = np.random.uniform(-1000.0, 1000.0)
        sigma1 = np.random.uniform(0, 100.0)
        mean2 = np.random.uniform(-1000.0, 1000.0)
        sigma2 = np.random.uniform(0, 100.0)
        size1 = np.random.randint(1, 10000)
        size2 = np.random.randint(1, 10000)
        a = np.append(np.random.normal(mean1, sigma1, size=size1),
                      np.random.normal(mean2, sigma2, size=size2))
        mask = np.zeros(size1 + size2, dtype=np.uint8)
        mask[np.random.randint(0, size1 + size2 - 1, size=np.random.randint(1, size1 + size2 - 1))] = 1
        _compare_median1d(a, mask)


def test_median1d_bimodal_uniformal_distribution_mask():
    for i in range(20):
        size1 = np.random.randint(1, 10000)
        size2 = np.random.randint(1, 10000)
        center1 = np.random.uniform(-10000.0, 10000.0)
        center2 = np.random.uniform(-10000.0, 10000.0)
        mask = np.zeros(size1 + size2, dtype=np.uint8)
        mask[np.random.randint(0, size1 + size2 - 1, size=np.random.randint(1, size1 + size2 - 1))] = 1
        a = np.append(np.random.normal(-1000.0, 1000.0, size=size1) + center1,
                      np.random.normal(-1000.0, 1000.0, size=size2) + center2)
        _compare_median1d(a, mask)


def _compare_median2d(a, mask):
    actual = median_utils.median2d(a.astype('f4'), mask.astype(np.uint8))

    mask_array = mask == 0
    n_good_pixels = mask_array.sum(axis=1)[0]
    d = a[mask == 0].reshape(a.shape[0], n_good_pixels)
    expected = np.median(d.astype('f4'), axis=1)
    np.testing.assert_allclose(actual, np.float32(expected))


def test_median2d_arange_nomask():
    for i in range(5):
        size_x, size_y = np.random.randint(1, 200), np.random.randint(1, 200)
        a = np.arange(size_x * size_y, dtype=np.float32).reshape(size_y, size_x)
        np.random.shuffle(a)
        _compare_median2d(a, np.zeros((size_y, size_x), dtype=np.uint8))


def test_median2d_normal_distribution_nomask():
    for i in range(5):
        mean = np.random.uniform(-1000.0, 1000.0)
        sigma = np.random.uniform(0, 100.0)
        size_x, size_y = np.random.randint(1, 200), np.random.randint(1, 200)
        a = np.random.normal(mean, sigma, size=(size_y, size_x))
        _compare_median2d(a.astype('f4'), np.zeros((size_y, size_x), dtype=np.uint8))


def test_median2d_uniform_distribution_nomask():
    for i in range(5):
        size_x, size_y = np.random.randint(1, 200), np.random.randint(1, 200)
        a = np.random.uniform(-1000.0, 1000.0, size=(size_y, size_x))
        _compare_median2d(a.astype('f4'), np.zeros((size_y, size_x), dtype=np.uint8))


# def test_median2d_bimodel_arange_nomask():
#     for i in range(100):
#         size1 = np.random.randint(1, 10000)
#         size2 = np.random.randint(1, 10000)
#         start1 = np.random.choice(10000)
#         start2 = np.random.choice(10000)
#         a = np.append(np.arange(size1, dtype=np.float32) + start1,
#                       np.arange(size2, dtype=np.float32) + start2)
#         _compare_median2d(a, np.zeros(size1 + size2, dtype=np.uint8))
#
#
# def test_median2d_bimodal_normal_distribution_nomask():
#     for i in range(100):
#         mean1 = np.random.uniform(-1000.0, 1000.0)
#         sigma1 = np.random.uniform(0, 100.0)
#         mean2 = np.random.uniform(-1000.0, 1000.0)
#         sigma2 = np.random.uniform(0, 100.0)
#         size1 = np.random.randint(1, 10000)
#         size2 = np.random.randint(1, 10000)
#         a = np.append(np.random.normal(mean1, sigma1, size=size1),
#                       np.random.normal(mean2, sigma2, size=size2))
#         _compare_median2d(a, np.zeros(size1 + size2, dtype=np.uint8))
#
#
# def test_median2d_bimodal_uniformal_distribution_nomask():
#     for i in range(100):
#         size1 = np.random.randint(1, 10000)
#         size2 = np.random.randint(1, 10000)
#         center1 = np.random.uniform(-10000.0, 10000.0)
#         center2 = np.random.uniform(-10000.0, 10000.0)
#         a = np.append(np.random.normal(-1000.0, 1000.0, size=size1) + center1,
#                       np.random.normal(-1000.0, 1000.0, size=size2) + center2)
#         _compare_median2d(a, np.zeros(size1 + size2, dtype=np.uint8))
#
#
# def test_median2d_all_masks_returns_zero():
#     size = 1000
#     a = np.zeros(size, dtype=np.float32)
#     mask = np.ones(size, dtype=np.uint8)
#     assert stats._median2d(a, mask) == 0.0
#
#
# def test_median2d_arange_mask():
#     for i in range(100):
#         size = np.random.choice(10000)
#         a = np.arange(size, dtype=np.float32)
#         np.random.shuffle(a)
#         mask = np.zeros(size, dtype=np.uint8)
#         mask[np.random.choice(size - 1, size=np.random.randint(1, size - 1))] = 1
#         _compare_median2d(a, mask)
#
#
# def test_median2d_normal_distribution_mask():
#     for i in range(100):
#         mean = np.random.uniform(-1000.0, 1000.0)
#         sigma = np.random.uniform(0, 100.0)
#         size = np.random.choice(10000)
#         a = np.random.normal(mean, sigma, size=size)
#         mask = np.zeros(size, dtype=np.uint8)
#         mask[np.random.choice(size - 1, size=np.random.randint(1, size - 1))] = 1
#         _compare_median2d(a.astype('f4'), mask)
#
#
# def test_median2d_uniform_distribution_mask():
#     for i in range(100):
#         size = np.random.choice(10000)
#         a = np.random.uniform(-1000.0, 1000.0, size=size)
#         mask = np.zeros(size, dtype=np.uint8)
#         mask[np.random.choice(size - 1, size=np.random.randint(1, size - 1))] = 1
#         _compare_median2d(a.astype('f4'), mask)
#
#
# def test_median2d_bimodel_arange_mask():
#     for i in range(100):
#         size1 = np.random.randint(1, 10000)
#         size2 = np.random.randint(1, 10000)
#         start1 = np.random.choice(10000)
#         start2 = np.random.choice(10000)
#         mask = np.zeros(size1 + size2, dtype=np.uint8)
#         mask[np.random.choice(size1 + size2 - 1, size=np.random.randint(1, size1 + size2 - 1))] = 1
#         a = np.append(np.arange(size1, dtype=np.float32) + start1,
#                       np.arange(size2, dtype=np.float32) + start2)
#         _compare_median2d(a, mask)
#
#
# def test_median2d_bimodal_normal_distribution_mask():
#     for i in range(100):
#         mean1 = np.random.uniform(-1000.0, 1000.0)
#         sigma1 = np.random.uniform(0, 100.0)
#         mean2 = np.random.uniform(-1000.0, 1000.0)
#         sigma2 = np.random.uniform(0, 100.0)
#         size1 = np.random.randint(1, 10000)
#         size2 = np.random.randint(1, 10000)
#         a = np.append(np.random.normal(mean1, sigma1, size=size1),
#                       np.random.normal(mean2, sigma2, size=size2))
#         mask = np.zeros(size1 + size2, dtype=np.uint8)
#         mask[np.random.choice(size1 + size2 - 1, size=np.random.randint(1, size1 + size2 - 1))] = 1
#         _compare_median2d(a, mask)
#
#
# def test_median2d_bimodal_uniformal_distribution_mask():
#     for i in range(100):
#         size1 = np.random.randint(1, 10000)
#         size2 = np.random.randint(1, 10000)
#         center1 = np.random.uniform(-10000.0, 10000.0)
#         center2 = np.random.uniform(-10000.0, 10000.0)
#         mask = np.zeros(size1 + size2, dtype=np.uint8)
#         mask[np.random.choice(size1 + size2 - 1, size=np.random.randint(1, size1 + size2 - 1))] = 1
#         a = np.append(np.random.normal(-1000.0, 1000.0, size=size1) + center1,
#                       np.random.normal(-1000.0, 1000.0, size=size2) + center2)
#         _compare_median2d(a, mask)
#
