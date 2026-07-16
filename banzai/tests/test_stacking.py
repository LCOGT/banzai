import numpy as np
import pytest
from astropy.io import fits

from banzai import data as data_module
from banzai.data import CCDData, stack
from banzai.tests.utils import FakeCCDData, pre_refactor_stack_snapshot

pytestmark = pytest.mark.stacking


def _make_ccd(data, mask=None, uncertainty=None):
    data = np.asarray(data, dtype=np.float64)
    meta = fits.Header({'DATASEC': f'[1:{data.shape[1]},1:{data.shape[0]}]',
                        'DETSEC': f'[1:{data.shape[1]},1:{data.shape[0]}]',
                        'CCDSUM': '1 1'})
    if mask is None:
        mask = np.zeros(data.shape, dtype=np.uint8)
    if uncertainty is None:
        uncertainty = np.ones(data.shape, dtype=data.dtype)
    return FakeCCDData(data=data, mask=np.asarray(mask, dtype=np.uint8), uncertainty=np.asarray(uncertainty), meta=meta)


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
    d = np.arange(nx*ny, dtype=np.float64).reshape(ny, nx)
    test_data = [FakeCCDData(data=d * i,
                             mask=np.zeros((ny, nx), dtype=np.uint8),
                             uncertainty=np.ones((ny, nx)) * 3.0) for i in range(9)]

    stacked_data = stack(test_data, 1e5)
    np.testing.assert_allclose(stacked_data.data, 4.0 * d)
    np.testing.assert_allclose(stacked_data.uncertainty, np.ones((ny, nx)))
    assert np.all(stacked_data.mask == 0)


def test_stack_mean_matches_pre_refactor_snapshot():
    rng = np.random.default_rng(318209)
    data_to_stack = []
    for i in range(7):
        data = rng.normal(loc=100.0 + i, scale=2.0, size=(6, 8))
        mask = np.zeros(data.shape, dtype=np.uint8)
        uncertainty = rng.uniform(0.5, 2.5, size=data.shape)
        data_to_stack.append(_make_ccd(data, mask=mask, uncertainty=uncertainty))

    data_to_stack[0].data[2, 3] = 1000.0
    data_to_stack[1].mask[1, 2] = 1
    data_to_stack[3].mask[4, 5] = 2
    data_to_stack[5].mask[0, 0] = 4

    expected = pre_refactor_stack_snapshot(data_to_stack, 3.0)
    actual_default = stack(data_to_stack, 3.0)

    assert np.array_equal(actual_default.data, expected.data)
    assert np.array_equal(actual_default.uncertainty, expected.uncertainty)
    assert np.array_equal(actual_default.mask, expected.mask)

    actual_mean = stack(data_to_stack, 3.0, method='mean')
    assert np.array_equal(actual_mean.data, expected.data)
    assert np.array_equal(actual_mean.uncertainty, expected.uncertainty)
    assert np.array_equal(actual_mean.mask, expected.mask)


def test_stack_sum_equals_mean_times_n_when_nothing_rejected():
    data_to_stack = [_make_ccd(np.ones((3, 4)) * value) for value in [2.0, 4.0, 6.0, 8.0]]

    stacked_mean = stack(data_to_stack, 1e5, method='mean')
    stacked_sum = stack(data_to_stack, 1e5, method='sum')

    np.testing.assert_allclose(stacked_sum.data, stacked_mean.data * len(data_to_stack))


def test_stack_sum_uncertainty_scaling():
    n_images = 5
    sigma = 3.25
    data_to_stack = [_make_ccd(np.ones((3, 4)), uncertainty=np.ones((3, 4)) * sigma) for _ in range(n_images)]

    stacked_sum = stack(data_to_stack, 1e5, method='sum')

    np.testing.assert_allclose(stacked_sum.uncertainty, np.ones((3, 4)) * np.sqrt(n_images) * sigma)


def test_stack_rejects_outlier():
    data_to_stack = [_make_ccd(np.ones((2, 2)) * 10.0) for _ in range(5)]
    data_to_stack[2].data[0, 0] = 1010.0

    stacked_sum = stack(data_to_stack, 3.0, method='sum')

    assert stacked_sum.data[0, 0] == 50.0


def test_stack_all_bad_pixel():
    data_to_stack = [
        _make_ccd([[1.0, 10.0]], mask=[[1, 0]], uncertainty=[[0.5, 1.0]]),
        _make_ccd([[2.0, 10.0]], mask=[[2, 0]], uncertainty=[[0.5, 1.0]]),
        _make_ccd([[3.0, 10.0]], mask=[[4, 0]], uncertainty=[[0.5, 1.0]]),
    ]

    stacked_sum = stack(data_to_stack, 3.0, method='sum')

    assert stacked_sum.mask[0, 0] == 7
    assert np.isfinite(stacked_sum.data[0, 0])
    assert np.isfinite(stacked_sum.uncertainty[0, 0])


def test_stack_n1_and_n2_do_not_reject():
    one_image = [_make_ccd([[1000.0]])]
    two_images = [_make_ccd([[0.0]]), _make_ccd([[1000.0]])]

    assert stack(one_image, 3.0, method='sum').data[0, 0] == 1000.0
    assert stack(two_images, 3.0, method='sum').data[0, 0] == 1000.0


def test_stack_unknown_method_raises():
    data_to_stack = [_make_ccd([[1.0]]), _make_ccd([[2.0]])]

    with pytest.raises(ValueError):
        stack(data_to_stack, 3.0, method='median')


def test_combine_images_matches_whole_array():
    rng = np.random.default_rng(826190)
    data_to_stack = []
    for value in [1.0, 2.0, 3.0]:
        data_to_stack.append(_make_ccd(rng.normal(value, 0.1, size=(7, 4))))

    data_to_stack[0].data[5, 2] = 1000.0
    data_to_stack[1].mask[6, 1] = 1
    output_frame = type('OutputFrame', (), {})()
    output_frame.primary_hdu = _make_ccd(np.zeros((7, 4)))

    data_module.combine_images(data_to_stack, output_frame, nsigma=3.0, method='sum')
    expected = stack(data_to_stack, 3.0, method='sum')

    np.testing.assert_allclose(output_frame.primary_hdu.data, expected.data)
    np.testing.assert_allclose(output_frame.primary_hdu.uncertainty, expected.uncertainty)
    assert np.array_equal(output_frame.primary_hdu.mask, expected.mask)


def test_combine_images_warns_when_exposure_spread_exceeds_one_percent(monkeypatch):
    warnings = []

    class FakeLogger:
        def warning(self, message, extra_tags=None):
            warnings.append((message, extra_tags))

    data_to_stack = [_make_ccd(np.ones((2, 2)) * value) for value in [1.0, 2.0, 3.0]]
    for data, exptime in zip(data_to_stack, [10.0, 10.05, 10.2]):
        data.meta['EXPTIME'] = exptime

    output_frame = type('OutputFrame', (), {})()
    output_frame.primary_hdu = _make_ccd(np.zeros((2, 2)))
    monkeypatch.setattr(data_module, 'logger', FakeLogger(), raising=False)

    data_module.combine_images(data_to_stack, output_frame, nsigma=3.0, method='mean')

    assert len(warnings) == 1
    assert warnings[0][1]['exptime_spread_fraction'] > 0.01
