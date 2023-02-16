import pytest
import mock
import numpy as np

from banzai.readnoise import ReadNoiseLoader
from banzai.tests.utils import FakeContext, FakeLCOObservationFrame, FakeCCDData

pytestmark = pytest.mark.read_noise


@pytest.fixture(scope="module")
def set_random_seed():
    np.random.seed(81232385)


def make_test_readnoise(nx, ny, input_readnoise, make_3d=False):
    if make_3d:
        final_shape = (4, ny, nx)
    else:
        final_shape = (ny, nx)
    return np.random.normal(0.0, input_readnoise, final_shape)


def test_null_input_images():
    tester = ReadNoiseLoader(None)
    image = tester.run(None)
    assert image is None


@mock.patch("banzai.lco.LCOFrameFactory.open")
@mock.patch("banzai.calibrations.CalibrationUser.get_calibration_file_info", return_value={"filename": "test.fits"})
def test_adds_good_noise_map(mock_noise_map_file_info, mock_noise_map, set_random_seed):
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(memmap=False)])
    super_image = FakeLCOObservationFrame(
        hdu_list=[FakeCCDData(data=make_test_readnoise(101, 103, 9.0), memmap=False)], file_path="test.fits"
    )
    mock_noise_map.return_value = super_image
    tester = ReadNoiseLoader(FakeContext())
    image = tester.do_stage(image)
    np.testing.assert_array_equal(image.uncertainty, super_image.data)
    assert image.meta.get("L1IDNOISE") == "test.fits"


@mock.patch("banzai.lco.LCOFrameFactory.open")
@mock.patch("banzai.calibrations.CalibrationUser.get_calibration_file_info", return_value={"filename": "test.fits"})
def test_adds_good_noise_map_3d(mock_noise_map_file_info, mock_noise_map, set_random_seed):
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(memmap=False) for i in range(4)])
    super_image = FakeLCOObservationFrame(
        hdu_list=[
            FakeCCDData(data=readnoise_data, memmap=False)
            for readnoise_data in make_test_readnoise(101, 103, 9.0, make_3d=True)
        ],
        file_path="test.fits",
    )

    mock_noise_map.return_value = super_image
    tester = ReadNoiseLoader(FakeContext())
    image = tester.do_stage(image)
    for image_hdu, master_hdu in zip(image.ccd_hdus, super_image.ccd_hdus):
        np.testing.assert_array_equal(image_hdu.uncertainty, master_hdu.data)
    assert image.meta.get("L1IDNOISE") == "test.fits"


@mock.patch("banzai.calibrations.CalibrationUser.get_calibration_file_info", return_value=None)
def test_no_noise_map(mock_noise_map_file_info, set_random_seed):
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(memmap=False) for i in range(4)])
    starting_uncertainty = image.uncertainty

    tester = ReadNoiseLoader(FakeContext())
    image = tester.do_stage(image)

    np.testing.assert_array_equal(image.uncertainty, starting_uncertainty)


@mock.patch("banzai.calibrations.CalibrationUser.get_calibration_file_info", return_value=None)
def test_no_noise_map_3d(mock_noise_map_file_info, set_random_seed):
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(memmap=False) for i in range(4)])
    starting_uncertainties = [ccd_data.uncertainty for ccd_data in image.ccd_hdus]

    tester = ReadNoiseLoader(FakeContext())
    image = tester.do_stage(image)

    # make sure uncertanties aren't changed
    for image_hdu, uncertainty in zip(image.ccd_hdus, starting_uncertainties):
        np.testing.assert_array_equal(image_hdu.uncertainty, uncertainty)
