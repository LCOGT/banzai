import pytest
import mock
import numpy as np

from banzai.flats import FlatComparer
from banzai.tests.utils import throws_inhomogeneous_set_exception, FakeContext
from banzai.tests.flat_utils import FakeFlatImage, make_context_with_master_flat


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(791873249)


def test_null_input_image():
    comparer = FlatComparer(FakeContext())
    image = comparer.run(None)
    assert image is None


def test_master_selection_criteria():
    comparer = FlatComparer(FakeContext())
    assert comparer.master_selection_criteria == ['ccdsum', 'filter']


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_ccdsums_are_different(mock_cal):
    throws_inhomogeneous_set_exception(FlatComparer, FakeContext(), 'ccdsum', '1 1')


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_nx_are_different(mock_cal):
    throws_inhomogeneous_set_exception(FlatComparer, FakeContext(), 'nx', 105)


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_ny_are_different(mock_cal):
    throws_inhomogeneous_set_exception(FlatComparer, FakeContext(), 'ny', 107)


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_filters_are_different(mock_cal):
    throws_inhomogeneous_set_exception(FlatComparer, FakeContext(), 'filter', 'w')


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_flag_bad_if_no_master_calibration( mock_cal, set_random_seed):
    mock_cal.return_value = None
    context = make_context_with_master_flat(flat_level=10000.0)
    comparer = FlatComparer(context)
    image = comparer.do_stage(FakeFlatImage(10000.0))
    assert image.is_bad is True


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_does_not_flag_noisy_images(mock_cal, set_random_seed):
    mock_cal.return_value = 'test.fits'
    master_flat_variation = 0.025
    nx = 101
    ny = 103
    flat_level = 10000.0

    context = make_context_with_master_flat(flat_level=1.0, master_flat_variation=master_flat_variation, nx=nx, ny=ny)
    image = FakeFlatImage(flat_level=flat_level)
    image.data = np.random.poisson(flat_level * np.ones((ny, nx))).astype(float)
    image.data += np.random.normal(0.0, image.readnoise)
    image.data /= flat_level
    comparer = FlatComparer(context)
    image = comparer.do_stage(image)

    assert image.is_bad is False


# Turn on image rejection for Flats. In the long term, this can be removed.
class FakeFlatComparer(FlatComparer):
    @property
    def reject_images(self):
        return True


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_does_flag_bad_images(mock_cal, set_random_seed):
    mock_cal.return_value = 'test.fits'
    nx = 101
    ny = 103
    flat_level = 10000.0
    master_flat_variation = 0.05

    context = make_context_with_master_flat(flat_level=flat_level, master_flat_variation=master_flat_variation,
                                            nx=nx, ny=ny)
    comparer = FakeFlatComparer(context)
    image = FakeFlatImage(flat_level)
    image.data = np.random.poisson(flat_level * context.FRAME_CLASS().data).astype(float)
    image.data += np.random.normal(0.0, image.readnoise)
    image.data /= flat_level

    # Make 20% of the image 20% brighter
    xinds = np.random.choice(np.arange(nx), size=int(0.2 * nx * ny), replace=True)
    yinds = np.random.choice(np.arange(ny), size=int(0.2 * nx * ny), replace=True)
    for x, y in zip(xinds, yinds):
        image.data[y, x] *= 1.2
    image = comparer.do_stage(image)

    assert image.is_bad is True
