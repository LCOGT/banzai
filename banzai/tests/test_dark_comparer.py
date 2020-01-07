import pytest
import mock
import numpy as np

from banzai.dark import DarkComparer
from banzai.tests.utils import FakeContext, FakeLCOObservationFrame, FakeCCDData
from banzai.tests.dark_utils import make_realistic_master_dark, get_dark_pattern

pytestmark = pytest.mark.dark_comparer


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(6234585)


def test_null_input_image():
    comparer = DarkComparer(FakeContext())
    image = comparer.run(None)
    assert image is None


def test_master_selection_criteria():
    comparer = DarkComparer(FakeContext())
    assert comparer.master_selection_criteria == ['configuration_mode', 'binning']


@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_filename')
def test_flags_bad_if_no_master_calibration(mock_master_filename, set_random_seed):
    image = FakeLCOObservationFrame()
    mock_master_filename.return_value = None
    comparer = DarkComparer(FakeContext())
    image = comparer.do_stage(image)
    assert image.is_bad is True


@mock.patch('banzai.images.LCOFrameFactory.open')
@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_filename')
def test_does_not_flag_noisy_images(mock_master_cal_name, mock_master_frame, set_random_seed):
    mock_master_cal_name.return_value = 'test.fits'
    master_dark_fraction = 0.05
    nx = 101
    ny = 103
    dark_exptime = 900.0
    image_readnoise = 11.0

    dark_pattern = get_dark_pattern(nx, ny, master_dark_fraction)
    mock_master_frame.return_value = make_realistic_master_dark(dark_pattern, nx=nx, ny=ny,
                                                                dark_level=30.0, dark_exptime=dark_exptime)
    comparer = DarkComparer(FakeContext())
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta={'EXPTIME': dark_exptime},
                                                          read_noise=image_readnoise)])
    image.primary_hdu.data = np.random.normal(0.0, image.primary_hdu.read_noise, size=(ny, nx))
    image.primary_hdu.data += np.random.poisson(dark_pattern * dark_exptime, size=(ny, nx))
    image.primary_hdu.data /= image.exptime
    image.primary_hdu.uncertainty /= image.exptime

    image = comparer.do_stage(image)

    assert image.is_bad is False


@mock.patch('banzai.images.LCOFrameFactory.open')
@mock.patch('banzai.calibrations.CalibrationUser.get_calibration_filename')
def test_does_flag_bad_images(mock_master_cal_name, mock_master_frame, set_random_seed):
    mock_master_cal_name.return_value = 'test.fits'
    master_dark_fraction = 0.05
    nx = 101
    ny = 103
    dark_exptime = 900.0
    dark_level = 10.0

    dark_pattern = get_dark_pattern(nx, ny, master_dark_fraction)
    mock_master_frame.return_value = make_realistic_master_dark(dark_pattern, nx=nx, ny=ny, dark_level=30.0,
                                                                dark_exptime=dark_exptime)

    comparer = DarkComparer(FakeContext())
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(meta={'EXPTIME': dark_exptime},
                                                          read_noise=11.0)])
    image.primary_hdu.data = np.random.normal(dark_level, image.primary_hdu.read_noise, size=(ny, nx))
    image.primary_hdu.data += np.random.poisson(dark_pattern * dark_exptime)
    image.primary_hdu.data /= image.exptime
    image.primary_hdu.uncertainty /= image.exptime

    # Make 20% of the image 10 times as bright
    xinds = np.random.choice(np.arange(nx), size=int(0.2 * nx * ny), replace=True)
    yinds = np.random.choice(np.arange(ny), size=int(0.2 * nx * ny), replace=True)
    for x, y in zip(xinds, yinds):
        image.primary_hdu.data[y, x] *= 10.0
    image = comparer.do_stage(image)

    assert image.is_bad is True
