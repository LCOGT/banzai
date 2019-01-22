import pytest
import mock
import numpy as np

from banzai.flats import FlatComparer
from banzai.tests.utils import throws_inhomogeneous_set_exception, FakeContext
from banzai.tests.flat_utils import FakeFlatImage, make_context_with_master_flat


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(791873249)


def test_no_input_images(set_random_seed):
    comparer = FlatComparer(FakeContext())
    images = comparer.do_stage([])
    assert len(images) == 0


def test_master_selection_criteria(set_random_seed):
    comparer = FlatComparer(FakeContext())
    assert comparer.master_selection_criteria == ['ccdsum', 'filter']


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_ccdsums_are_different(mock_cal, set_random_seed):
    throws_inhomogeneous_set_exception(FlatComparer, FakeContext(), 'ccdsum', '1 1')


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_epochs_are_different(mock_cal, set_random_seed):
    throws_inhomogeneous_set_exception(FlatComparer, FakeContext(), 'epoch', '20160102')


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_nx_are_different(mock_cal, set_random_seed):
    mock_cal.return_value = 'test.fits'
    throws_inhomogeneous_set_exception(FlatComparer, FakeContext(), 'nx', 105)


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_ny_are_different(mock_cal, set_random_seed):
    mock_cal.return_value = 'test.fits'
    throws_inhomogeneous_set_exception(FlatComparer, FakeContext(), 'ny', 107)


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
@mock.patch('banzai.stages.Stage.save_qc_results')
def test_does_not_raise_exception_if_no_master_calibration(mock_save_qc, mock_cal, set_random_seed):
    mock_cal.return_value = None
    context = make_context_with_master_flat(flat_level=10000.0)
    comparer = FlatComparer(context)
    images = comparer.do_stage([FakeFlatImage(flat_level=10000.0) for x in range(6)])
    assert len(images) == 6


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
@mock.patch('banzai.stages.Stage.save_qc_results')
def test_does_not_reject_noisy_images(mock_save_qc, mock_cal, set_random_seed):
    mock_cal.return_value = 'test.fits'
    master_flat_variation = 0.05
    nx = 101
    ny = 103
    flat_level = 10000.0

    context = make_context_with_master_flat(flat_level=1.0, master_flat_variation=master_flat_variation, nx=nx, ny=ny)
    comparer = FlatComparer(context)
    images = [FakeFlatImage(flat_level=flat_level) for _ in range(6)]
    for image in images:
        image.data = np.random.poisson(flat_level * np.ones((ny, nx))).astype(float)
        image.data += np.random.normal(0.0, image.readnoise)
        image.data /= flat_level

    images = comparer.do_stage(images)

    assert len(images) == 6


# Turn on image rejection for Flats. In the long term, this can be removed.
class FakeFlatComparer(FlatComparer):
    @property
    def reject_images(self):
        return True


@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
@mock.patch('banzai.stages.Stage.save_qc_results')
def test_does_reject_bad_images(mock_save_qc, mock_cal, set_random_seed):
    mock_cal.return_value = 'test.fits'
    nx = 101
    ny = 103
    flat_level = 10000.0
    master_flat_variation = 0.05

    context = make_context_with_master_flat(flat_level=flat_level, master_flat_variation=master_flat_variation,
                                            nx=nx, ny=ny)
    comparer = FakeFlatComparer(context)
    images = [FakeFlatImage(flat_level) for _ in range(6)]
    for image in images:
        image.data = np.random.poisson(flat_level * context.FRAME_CLASS().data).astype(float)
        image.data += np.random.normal(0.0, image.readnoise)
        image.data /= flat_level

    for i in [3, 5]:
        # Make 20% of the image 20% brighter
        xinds = np.random.choice(np.arange(nx), size=int(0.2 * nx * ny), replace=True)
        yinds = np.random.choice(np.arange(ny), size=int(0.2 * nx * ny), replace=True)
        for x, y in zip(xinds, yinds):
            images[i].data[y, x] *= 1.2
    images = comparer.do_stage(images)

    assert len(images) == 4
