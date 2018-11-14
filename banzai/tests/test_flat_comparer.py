import pytest
import mock
import numpy as np

from banzai.flats import FlatComparer
from banzai.tests.utils import FakeImage, throws_inhomogeneous_set_exception, FakeContext


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(791873249)


class FakeFlatImage(FakeImage):
    def __init__(self, flat_level, **kwargs):
        super(FakeFlatImage, self).__init__(**kwargs)
        self.header['FLATLVL'] = flat_level


def test_no_input_images(set_random_seed):
    comparer = FlatComparer(None)
    images = comparer.do_stage([])
    assert len(images) == 0


def test_master_selection_criteria(set_random_seed):
    comparer = FlatComparer(FakeContext())
    assert comparer.master_selection_criteria == ['ccdsum', 'filter']


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_ccdsums_are_different(mock_cal, mock_images, set_random_seed):
    throws_inhomogeneous_set_exception(FlatComparer, FakeContext(), 'ccdsum', '1 1')


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_epochs_are_different(mock_cal, mock_images, set_random_seed):
    throws_inhomogeneous_set_exception(FlatComparer, FakeContext(), 'epoch', '20160102')


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_nx_are_different(mock_cal, mock_images, set_random_seed):
    mock_cal.return_value = 'test.fits'
    throws_inhomogeneous_set_exception(FlatComparer, FakeContext(), 'nx', 105)


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
def test_raises_an_exception_if_ny_are_different(mock_cal, mock_images, set_random_seed):
    mock_cal.return_value = 'test.fits'
    throws_inhomogeneous_set_exception(FlatComparer, FakeContext(), 'ny', 107)


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
@mock.patch('banzai.stages.Stage.save_qc_results')
def test_does_not_raise_exception_if_no_master_calibration(mock_save_qc, mock_cal, mock_images, set_random_seed):
    mock_cal.return_value = None
    mock_images.return_value = FakeFlatImage(10000.0)

    comparer = FlatComparer(None)
    images = comparer.do_stage([FakeFlatImage(10000.0) for x in range(6)])
    assert len(images) == 6


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
@mock.patch('banzai.stages.Stage.save_qc_results')
def test_does_not_reject_noisy_images(mock_save_qc, mock_cal, mock_image, set_random_seed):
    mock_cal.return_value = 'test.fits'
    master_flat_variation = 0.05
    nx = 101
    ny = 103
    flat_level = 10000.0

    fake_master_flat = FakeFlatImage(flat_level=flat_level)
    fake_master_flat.data = np.random.normal(1.0, master_flat_variation, size=(ny, nx))
    mock_image.return_value = fake_master_flat

    comparer = FlatComparer(FakeContext())
    images = [FakeFlatImage(flat_level=flat_level) for _ in range(6)]
    for image in images:
        image.data = np.random.poisson(flat_level * fake_master_flat.data).astype(float)
        image.data += np.random.normal(0.0, image.readnoise)
        image.data /= flat_level

    images = comparer.do_stage(images)

    assert len(images) == 6


# Turn on image rejection for Flats. In the long term, this can be removed.
class FakeFlatComparer(FlatComparer):
    @property
    def reject_images(self):
        return True


@mock.patch('banzai.calibrations.Image')
@mock.patch('banzai.calibrations.ApplyCalibration.get_calibration_filename')
@mock.patch('banzai.stages.Stage.save_qc_results')
def test_does_reject_bad_images(mock_save_qc, mock_cal, mock_image, set_random_seed):
    mock_cal.return_value = 'test.fits'
    master_dark_fraction = 0.05
    nx = 101
    ny = 103
    flat_level = 10000.0
    master_flat_variation = 0.05

    fake_master_flat = FakeFlatImage(flat_level=flat_level)
    fake_master_flat.data = np.random.normal(1.0, master_flat_variation, size=(ny, nx))
    mock_image.return_value = fake_master_flat

    comparer = FakeFlatComparer(FakeContext())
    images = [FakeFlatImage(flat_level) for _ in range(6)]
    for image in images:
        image.data = np.random.poisson(flat_level * fake_master_flat.data).astype(float)
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
