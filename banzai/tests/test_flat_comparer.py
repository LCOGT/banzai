import pytest
import mock
import numpy as np

from banzai.flats import FlatComparer
from banzai.tests.utils import FakeContext, FakeCCDData, FakeLCOObservationFrame
from banzai.tests.flat_utils import make_realistic_master_flat

pytestmark = pytest.mark.flat_comparer


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(791873249)


def test_null_input_image():
    comparer = FlatComparer(FakeContext())
    image = comparer.run(None)
    assert image is None


def test_master_selection_criteria():
    comparer = FlatComparer(FakeContext())
    assert comparer.master_selection_criteria == ['configuration_mode', 'binning', 'filter']


@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_file_info')
def test_flag_bad_if_no_master_calibration(mock_master_filename, set_random_seed):
    mock_master_filename.return_value = None
    comparer = FlatComparer(FakeContext())
    image = comparer.do_stage(FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta={'FLATLVL': 10000})]))
    assert image.is_bad is True


@mock.patch('banzai.lco.LCOFrameFactory.open')
@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_file_info')
def test_does_not_flag_noisy_images(mock_master_filename, mock_master_frame, set_random_seed):
    mock_master_filename.return_value = {'filename': 'test.fits'}
    master_flat_variation = 0.025
    nx = 101
    ny = 103
    flat_level = 10000.0
    image_readnoise = 11.0

    mock_master_frame.return_value = make_realistic_master_flat(flat_level=1.0,
                                                                master_flat_variation=master_flat_variation,
                                                                nx=nx, ny=ny)

    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta={'FLATLVL': flat_level},
                                                          read_noise=image_readnoise)])

    image.primary_hdu.data = np.random.poisson(flat_level * np.ones((ny, nx))).astype(float)
    image.primary_hdu.data += np.random.normal(0.0, image_readnoise)
    image.primary_hdu.data /= flat_level
    image.primary_hdu.uncertainty /= flat_level

    comparer = FlatComparer(FakeContext())
    image = comparer.do_stage(image)

    assert image.is_bad is False


@mock.patch('banzai.lco.LCOFrameFactory.open')
@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_file_info')
def test_does_flag_bad_images(mock_master_filename, mock_master_frame, set_random_seed):
    mock_master_filename.return_value = {'filename': 'test.fits'}
    nx = 101
    ny = 103
    flat_level = 10000.0
    master_flat_variation = 0.05
    image_readnoise = 11.0

    mock_master_frame.return_value = make_realistic_master_flat(flat_level=flat_level,
                                                                master_flat_variation=master_flat_variation,
                                                                nx=nx, ny=ny)
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta={'FLATLVL': flat_level},
                                                          read_noise=image_readnoise)])
    image.primary_hdu.data = np.random.poisson(flat_level * np.ones((ny, nx))).astype(float)
    image.primary_hdu.data += np.random.normal(0.0, image_readnoise)
    image.primary_hdu.data /= flat_level
    image.primary_hdu.uncertainty /= flat_level

    # Make 20% of the image 20% brighter
    xinds = np.random.choice(np.arange(nx), size=int(0.2 * nx * ny), replace=True)
    yinds = np.random.choice(np.arange(ny), size=int(0.2 * nx * ny), replace=True)
    for x, y in zip(xinds, yinds):
        image.primary_hdu.data[y, x] *= 1.2

    comparer = FlatComparer(FakeContext())
    image = comparer.do_stage(image)

    assert image.is_bad is True
