from __future__ import absolute_import, division, print_function, unicode_literals
from banzai.utils import stats
import numpy as np
from numpy import ma
np.random.seed(10031312)


def test_median_axis_none_mask_none():
    for i in range(1000):
        size = np.random.randint(1, 10000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size)
        expected = np.median(a.astype(np.float32))
        actual = stats.median(a)
        assert np.float32(expected) == actual


def test_median_2d_axis_none_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2))
        expected = np.median(a.astype(np.float32))
        actual = stats.median(a)
        assert np.float32(expected) == actual


def test_median_3d_axis_none_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        expected = np.median(a.astype(np.float32))
        actual = stats.median(a)
        assert np.float32(expected) == actual


def test_median_2d_axis_0_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2))
        expected = np.median(a.astype(np.float32), axis=0)
        actual = stats.median(a, axis=0)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-6)


def test_median_2d_axis_1_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2))
        expected = np.median(a.astype(np.float32), axis=1)
        actual = stats.median(a, axis=1)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-6)


def test_median_3d_axis_0_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        expected = np.median(a.astype(np.float32), axis=0)
        actual = stats.median(a, axis=0)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-6)


def test_median_3d_axis_1_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        expected = np.median(a.astype(np.float32), axis=1)
        actual = stats.median(a, axis=1)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-6)


def test_median_3d_axis_2_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        expected = np.median(a.astype(np.float32), axis=2)
        actual = stats.median(a, axis=2)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-6)


def test_median_axis_none_mask():
    for i in range(1000):
        size = np.random.randint(1, 10000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size)
        value_to_mask = np.random.uniform(0, 1.0)
        mask = np.random.uniform(0, 1, size) < value_to_mask
        expected = ma.median(ma.array(a, mask=mask, dtype=np.float32))
        actual = stats.median(a, mask=mask)
        assert np.float32(expected) == actual


def test_median_2d_axis_none_mask():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        value_to_mask = np.random.uniform(0, 1)
        mask = np.random.uniform(0, 1, size=(size1, size2)) < value_to_mask
        a = np.random.normal(mean, sigma, size=(size1, size2))
        expected = ma.median(ma.array(a, mask=mask, dtype=np.float32))
        actual = stats.median(a, mask=mask)
        assert np.float32(expected) == actual


def test_median_3d_axis_none_mask():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        value_to_mask = np.random.uniform(0, 1)
        mask = np.random.uniform(0., 1.0, size=(size1, size2, size3)) < value_to_mask
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        expected = ma.median(ma.array(a, mask=mask, dtype=np.float32))
        actual = stats.median(a, mask=mask)
        assert np.float32(expected) == actual


def test_median_2d_axis_0_mask():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        value_to_mask = np.random.uniform(0, 1)
        mask = np.random.uniform(0., 1.0, size=(size1, size2)) < value_to_mask
        a = np.random.normal(mean, sigma, size=(size1, size2))
        expected = ma.median(ma.array(a, mask=mask, dtype=np.float32), axis=0)
        actual = stats.median(a, mask=mask, axis=0)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-6)


def test_median_2d_axis_1_mask():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        value_to_mask = np.random.uniform(0, 1)
        mask = np.random.uniform(0., 1.0, size=(size1, size2)) < value_to_mask
        a = np.random.normal(mean, sigma, size=(size1, size2))
        expected = ma.median(ma.array(a, mask=mask, dtype=np.float32), axis=1)
        actual = stats.median(a, mask=mask, axis=1)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-6)


def test_median_3d_axis_0_mask():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        value_to_mask = np.random.uniform(0, 1)
        mask = np.random.uniform(0, 1, size=(size1, size2, size3)) < value_to_mask
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        expected = ma.median(ma.array(a, mask=mask, dtype=np.float32), axis=0)
        actual = stats.median(a, mask=mask, axis=0)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-6)


def test_median_3d_axis_1_mask():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        value_to_mask = np.random.uniform(0, 1)
        mask = np.random.uniform(0, 1, size=(size1, size2, size3)) < value_to_mask
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        expected = ma.median(ma.array(a, mask=mask, dtype=np.float32), axis=1)
        actual = stats.median(a, mask=mask, axis=1)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-6)


def test_median_3d_axis_2_mask():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        value_to_mask = np.random.uniform(0, 1)
        mask = np.random.uniform(0, 1, size=(size1, size2, size3)) < value_to_mask
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        expected = ma.median(ma.array(a, mask=mask, dtype=np.float32), axis=2)
        actual = stats.median(a, mask=mask, axis=2)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-6)


def test_absolute_deviation_axis_none_mask_none():
    for i in range(1000):
        size = np.random.randint(1, 10000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size)
        expected = np.abs(a.astype(np.float32) - np.median(a.astype(np.float32)))
        actual = stats.absolute_deviation(a)
        np.testing.assert_allclose(actual, np.float32(expected), atol=1e-4)


def test_absolute_deviation_2d_axis_none_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2))
        expected = np.abs(a.astype(np.float32) - np.median(a.astype(np.float32)))
        actual = stats.absolute_deviation(a)
        np.testing.assert_allclose(actual, np.float32(expected), atol=1e-4)


def test_absolute_deviation_3d_axis_none_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        expected = np.abs(a.astype(np.float32) - np.median(a.astype(np.float32)))
        actual = stats.absolute_deviation(a)
        np.testing.assert_allclose(actual, np.float32(expected), atol=1e-4)


def test_absolute_deviation_2d_axis_0_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2))
        expected = np.abs(a.astype(np.float32) - np.median(a.astype(np.float32), axis=0))
        actual = stats.absolute_deviation(a, axis=0)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-4)


def test_absolute_deviation_2d_axis_1_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2))
        expected = np.abs(a.astype(np.float32).T - np.median(a.astype(np.float32), axis=1)).T
        actual = stats.absolute_deviation(a, axis=1)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-4)


def test_absolute_deviation_3d_axis_0_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        expected = np.abs(a.astype(np.float32) - np.median(a.astype(np.float32), axis=0))
        actual = stats.absolute_deviation(a, axis=0)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-4)


def test_absolute_deviation_3d_axis_1_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        expected = np.abs(a.astype(np.float32) - np.median(a.astype(np.float32), axis=1, keepdims=True))
        actual = stats.absolute_deviation(a, axis=1)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-4)


def test_absolute_deviation_3d_axis_2_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        expected = np.abs(a.astype(np.float32) - np.median(a.astype(np.float32), axis=2, keepdims=True))
        actual = stats.absolute_deviation(a, axis=2)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-4)


def test_absolute_deviation_axis_none_mask():
    for i in range(1000):
        size = np.random.randint(1, 10000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size)
        value_to_mask = np.random.uniform(0, 0.8)
        mask = np.random.uniform(0, 1.0, size) < value_to_mask
        a_masked = ma.array(a, mask=mask, dtype=np.float32)
        expected = np.abs(a_masked - ma.median(a_masked))
        actual = stats.absolute_deviation(a, mask=mask)
        np.testing.assert_allclose(actual, np.float32(expected), atol=1e-4)


def test_absolute_deviation_2d_axis_none_mask():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2))
        value_to_mask = np.random.uniform(0, 0.8)
        mask = np.random.uniform(0, 1.0, size=(size1, size2)) < value_to_mask
        a_masked = ma.array(a, mask=mask, dtype=np.float32)
        expected = np.abs(a - ma.median(a_masked))
        actual = stats.absolute_deviation(a, mask=mask)
        np.testing.assert_allclose(actual, np.float32(expected), atol=1e-4)


def test_absolute_deviation_3d_axis_none_mask():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        value_to_mask = np.random.uniform(0, 0.8)
        mask = np.random.uniform(0, 1.0, size=(size1, size2, size3)) < value_to_mask
        a_masked = ma.array(a, mask=mask, dtype=np.float32)
        expected = np.abs(a - ma.median(a_masked))
        actual = stats.absolute_deviation(a, mask=mask)
        np.testing.assert_allclose(actual, np.float32(expected), atol=1e-8)


def test_absolute_deviation_2d_axis_0_mask():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2))
        value_to_mask = np.random.uniform(0, 0.8)
        mask = np.random.uniform(0, 1.0, size=(size1, size2)) < value_to_mask
        a_masked = ma.array(a, mask=mask, dtype=np.float32)
        expected = np.abs(a - ma.median(a_masked, axis=0))
        actual = stats.absolute_deviation(a, mask=mask, axis=0)
        np.testing.assert_allclose(actual, np.float32(expected), atol=1e-4)


def test_absolute_deviation_2d_axis_1_mask():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2))
        value_to_mask = np.random.uniform(0, 0.8)
        mask = np.random.uniform(0, 1.0, size=(size1, size2)) < value_to_mask
        a_masked = ma.array(a, mask=mask, dtype=np.float32)
        expected = np.abs(a.T - ma.median(a_masked, axis=1)).T
        actual = stats.absolute_deviation(a, mask=mask, axis=1)
        np.testing.assert_allclose(actual, np.float32(expected), atol=1e-4)


def test_absolute_deviation_3d_axis_0_mask():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        value_to_mask = np.random.uniform(0, 0.8)
        mask = np.random.uniform(0, 1.0, size=(size1, size2, size3)) < value_to_mask
        a_masked = ma.array(a, mask=mask, dtype=np.float32)
        expected = np.abs(a - ma.median(a_masked, axis=0))
        actual = stats.absolute_deviation(a, mask=mask, axis=0)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-9)


def test_absolute_deviation_3d_axis_1_mask():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        value_to_mask = np.random.uniform(0, 0.8)
        mask = np.random.uniform(0, 1.0, size=(size1, size2, size3)) < value_to_mask
        a_masked = ma.array(a, mask=mask, dtype=np.float32)
        expected = np.abs(a - np.expand_dims(ma.median(a_masked, axis=1), axis=1))
        actual = stats.absolute_deviation(a, mask=mask, axis=1)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-9)


def test_absolute_deviation_3d_axis_2_mask():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        value_to_mask = np.random.uniform(0, 0.8)
        mask = np.random.uniform(0, 1.0, size=(size1, size2, size3)) < value_to_mask
        a_masked = ma.array(a, mask=mask, dtype=np.float32)
        expected = np.abs(a - np.expand_dims(ma.median(a_masked, axis=2), axis=2))
        actual = stats.absolute_deviation(a, mask=mask, axis=2)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-9)


def test_mad_axis_none_mask_none():
    for i in range(1000):
        size = np.random.randint(1, 10000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size)
        expected = np.median(np.abs(a.astype(np.float32) - np.median(a.astype(np.float32))))
        actual = stats.median_absolute_deviation(a)
        np.testing.assert_allclose(actual, np.float32(expected), atol=1e-4)


def test_mad_2d_axis_none_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2))
        expected = np.median(np.abs(a.astype(np.float32) - np.median(a.astype(np.float32))))
        actual = stats.median_absolute_deviation(a)
        np.testing.assert_allclose(actual, np.float32(expected), atol=1e-4)


def test_mad_3d_axis_none_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        expected = np.median(np.abs(a.astype(np.float32) - np.median(a.astype(np.float32))))
        actual = stats.median_absolute_deviation(a)
        np.testing.assert_allclose(actual, np.float32(expected), atol=1e-4)


def test_mad_2d_axis_0_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2))
        expected = np.median(np.abs(a.astype(np.float32) - np.median(a.astype(np.float32), axis=0)), axis=0)
        actual = stats.median_absolute_deviation(a, axis=0)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-4)


def test_mad_2d_axis_1_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2))
        expected = np.median(np.abs(a.astype(np.float32).T - np.median(a.astype(np.float32), axis=1)).T, axis=1)
        actual = stats.median_absolute_deviation(a, axis=1)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-4)


def test_mad_3d_axis_0_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        expected = np.median(np.abs(a.astype(np.float32) - np.median(a.astype(np.float32), axis=0)), axis=0)
        actual = stats.median_absolute_deviation(a, axis=0)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-4)


def test_mad_3d_axis_1_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        expected = np.median(np.abs(a.astype(np.float32) - np.median(a.astype(np.float32), axis=1, keepdims=True)), axis=1)
        actual = stats.median_absolute_deviation(a, axis=1)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-4)


def test_mad_3d_axis_2_mask_none():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        expected = np.median(np.abs(a.astype(np.float32) - np.median(a.astype(np.float32), axis=2, keepdims=True)), axis=2)
        actual = stats.median_absolute_deviation(a, axis=2)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-4)


def test_mad_axis_none_mask():
    for i in range(1000):
        size = np.random.randint(1, 10000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size)
        value_to_mask = np.random.uniform(0, 0.8)
        mask = np.random.uniform(0, 1.0, size) < value_to_mask
        a_masked = ma.array(a, mask=mask, dtype=np.float32)
        expected = ma.median(ma.array(np.abs(a_masked - ma.median(a_masked)), dtype=np.float32, mask=mask))
        actual = stats.median_absolute_deviation(a, mask=mask)
        np.testing.assert_allclose(actual, np.float32(expected), atol=1e-4)


def test_mad_2d_axis_none_mask():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2))
        value_to_mask = np.random.uniform(0, 0.8)
        mask = np.random.uniform(0, 1.0, size=(size1, size2)) < value_to_mask
        a_masked = ma.array(a, mask=mask, dtype=np.float32)
        expected = ma.median(ma.array(np.abs(a - ma.median(a_masked)), dtype=np.float32, mask=mask))
        actual = stats.median_absolute_deviation(a, mask=mask)
        np.testing.assert_allclose(actual, expected, atol=1e-4)


def test_mad_3d_axis_none_mask():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        value_to_mask = np.random.uniform(0, 0.8)
        mask = np.random.uniform(0, 1.0, size=(size1, size2, size3)) < value_to_mask
        a_masked = ma.array(a, mask=mask, dtype=np.float32)
        expected = ma.median(ma.array(np.abs(a - ma.median(a_masked)), dtype=np.float32, mask=mask))
        actual = stats.median_absolute_deviation(a, mask=mask)
        np.testing.assert_allclose(actual, np.float32(expected), atol=1e-8)


def test_mad_2d_axis_0_mask():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2))
        value_to_mask = np.random.uniform(0, 0.8)
        mask = np.random.uniform(0, 1.0, size=(size1, size2)) < value_to_mask
        a_masked = ma.array(a, mask=mask, dtype=np.float32)
        expected = ma.median(ma.array(np.abs(a - ma.median(a_masked, axis=0)), dtype=np.float32, mask=mask), axis=0)
        actual = stats.median_absolute_deviation(a, mask=mask, axis=0)
        np.testing.assert_allclose(actual, np.float32(expected), atol=1e-4)


def test_mad_2d_axis_1_mask():
    for i in range(10):
        size1 = np.random.randint(1, 1000)
        size2 = np.random.randint(1, 1000)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2))
        value_to_mask = np.random.uniform(0, 0.8)
        mask = np.random.uniform(0, 1.0, size=(size1, size2)) < value_to_mask
        a_masked = ma.array(a, mask=mask, dtype=np.float32)
        expected = ma.median(ma.array(np.abs(a.T - ma.median(a_masked, axis=1)).T, dtype=np.float32, mask=mask), axis=1)
        actual = stats.median_absolute_deviation(a, mask=mask, axis=1)
        np.testing.assert_allclose(actual, np.float32(expected), atol=1e-4)


def test_mad_3d_axis_0_mask():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        value_to_mask = np.random.uniform(0, 0.8)
        mask = np.random.uniform(0, 1.0, size=(size1, size2, size3)) < value_to_mask
        a_masked = ma.array(a, mask=mask, dtype=np.float32)
        expected = ma.median(ma.array(np.abs(a - ma.median(a_masked, axis=0)), dtype=np.float32, mask=mask), axis=0)
        actual = stats.median_absolute_deviation(a, mask=mask, axis=0)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-9)


def test_mad_3d_axis_1_mask():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        value_to_mask = np.random.uniform(0, 0.8)
        mask = np.random.uniform(0, 1.0, size=(size1, size2, size3)) < value_to_mask
        a_masked = ma.array(a, mask=mask, dtype=np.float32)
        expected = ma.median(ma.array(np.abs(a - np.expand_dims(ma.median(a_masked, axis=1), axis=1)), dtype=np.float32, mask=mask), axis=1)
        actual = stats.median_absolute_deviation(a, mask=mask, axis=1)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-9)


def test_mad_3d_axis_2_mask():
    for i in range(10):
        size1 = np.random.randint(1, 100)
        size2 = np.random.randint(1, 100)
        size3 = np.random.randint(1, 100)
        mean = np.random.uniform(-1000, 1000)
        sigma = np.random.uniform(0, 1000)
        a = np.random.normal(mean, sigma, size=(size1, size2, size3))
        value_to_mask = np.random.uniform(0, 0.8)
        mask = np.random.uniform(0, 1.0, size=(size1, size2, size3)) < value_to_mask
        a_masked = ma.array(a, mask=mask, dtype=np.float32)
        expected = ma.median(ma.array(np.abs(a - np.expand_dims(ma.median(a_masked, axis=2), axis=2)), dtype=np.float32, mask=mask), axis=2)
        actual = stats.median_absolute_deviation(a, mask=mask, axis=2)
        np.testing.assert_allclose(actual, expected.astype(np.float32), atol=1e-9)


# def test_rstd_axis_none_mask_none():
#     for i in range(1000):
#         size = np.random.randint(1, 10000)
#         mean = np.random.uniform(-1000, 1000)
#         sigma = np.random.uniform(0, 1000)
#         a = np.random.normal(mean, sigma, size)
#         expected = np.std(a)
#         actual = stats.robust_standard_deviation(a)
#         np.testing.assert_allclose(actual, np.float32(expected), atol= sigma / (size ** 0.5))
