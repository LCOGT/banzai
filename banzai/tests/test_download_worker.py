import io
import os
import time
from unittest import mock

import pytest
from sqlalchemy import text

from banzai.cache.download_worker import DownloadWorker, CachedCalibrationInfo
from banzai.exceptions import FrameNotAvailableError
from banzai.tests.utils import FakeContext, FakeCacheConfig

pytestmark = pytest.mark.download_worker


class FakeCalibrationImage:
    """Fake calibration image database record for testing."""
    def __init__(self, id=1, filename='test_bias.fits', type='BIAS', frameid=12345,
                 instrument_id=1, filepath=None, attributes=None):
        self.id = id
        self.filename = filename
        self.type = type
        self.frameid = frameid
        self.instrument_id = instrument_id
        self.filepath = filepath
        self.attributes = attributes or {'configuration_mode': 'default', 'binning': '1x1'}


def make_worker(tmp_path):
    """Create a DownloadWorker with a temporary cache directory."""
    return DownloadWorker(
        db_address='sqlite:///test.db',
        cache_root=str(tmp_path),
        runtime_context=FakeContext()
    )


# --- get_cache_config tests ---

@mock.patch('banzai.dbs.get_session')
def test_get_cache_config_returns_config_when_exists(mock_get_session, tmp_path):
    fake_config = FakeCacheConfig()
    mock_session = mock.MagicMock()
    mock_session.query.return_value.first.return_value = fake_config
    mock_get_session.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

    worker = make_worker(tmp_path)
    result = worker.get_cache_config()

    assert result.site_id == 'cpt'
    assert result.instrument_types == ['*']


@mock.patch('banzai.dbs.get_session')
def test_get_cache_config_returns_none_when_empty(mock_get_session, tmp_path):
    mock_session = mock.MagicMock()
    mock_session.query.return_value.first.return_value = None
    mock_get_session.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

    worker = make_worker(tmp_path)
    result = worker.get_cache_config()

    assert result is None


# --- get_calibrations_to_cache tests ---

@mock.patch.object(DownloadWorker, 'get_cache_config')
def test_get_calibrations_to_cache_returns_empty_when_no_config(mock_get_config, tmp_path):
    mock_get_config.return_value = None

    worker = make_worker(tmp_path)
    result = worker.get_calibrations_to_cache()

    assert result == {}


@mock.patch('banzai.dbs.get_session')
@mock.patch.object(DownloadWorker, 'get_cache_config')
def test_get_calibrations_to_cache_handles_db_errors(mock_get_config, mock_get_session, tmp_path):
    mock_get_config.return_value = FakeCacheConfig()
    mock_session = mock.MagicMock()
    mock_session.execute.side_effect = Exception("Database error")
    mock_get_session.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

    worker = make_worker(tmp_path)
    result = worker.get_calibrations_to_cache()

    assert result == {}


# --- get_cached_files tests ---

def test_get_cached_files_returns_empty_set_for_nonexistent_dir(tmp_path):
    worker = DownloadWorker(
        db_address='sqlite:///test.db',
        cache_root=str(tmp_path / 'nonexistent'),
        runtime_context=FakeContext()
    )
    result = worker.get_cached_files()
    assert result == set()


def test_get_cached_files_returns_fits_files_only(tmp_path):
    # Create various files
    (tmp_path / 'valid1.fits').touch()
    (tmp_path / 'valid2.fits.fz').touch()
    (tmp_path / 'readme.txt').touch()
    (tmp_path / 'data.json').touch()
    (tmp_path / 'temp.fits.tmp').touch()

    worker = make_worker(tmp_path)
    result = worker.get_cached_files()

    assert result == {'valid1.fits', 'valid2.fits.fz'}


def test_get_cached_files_returns_empty_for_empty_dir(tmp_path):
    worker = make_worker(tmp_path)
    result = worker.get_cached_files()
    assert result == set()


# --- download_calibration tests ---

@mock.patch('banzai.dbs.get_session')
def test_download_calibration_skips_when_file_exists(mock_get_session, tmp_path):
    # Create an existing file
    existing_file = tmp_path / 'existing.fits'
    existing_file.touch()

    mock_session = mock.MagicMock()
    mock_cal = FakeCalibrationImage(filename='existing.fits')
    mock_session.query.return_value.get.return_value = mock_cal
    mock_get_session.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

    worker = make_worker(tmp_path)
    cal_info = CachedCalibrationInfo(
        id=1, filename='existing.fits', type='BIAS',
        frameid=12345, instrument_id=1, attributes={}
    )

    # Should not raise, should update database
    worker.download_calibration(cal_info)
    mock_session.query.return_value.get.assert_called_with(1)


def test_download_calibration_skips_when_frameid_is_none(tmp_path):
    worker = make_worker(tmp_path)
    cal_info = CachedCalibrationInfo(
        id=1, filename='null_frameid.fits', type='BIAS',
        frameid=None, instrument_id=1, attributes={}
    )

    # Should not raise, should skip silently
    worker.download_calibration(cal_info)

    # File should not be created
    assert not (tmp_path / 'null_frameid.fits').exists()


@mock.patch('banzai.dbs.get_session')
@mock.patch('banzai.utils.fits_utils.download_from_s3')
def test_download_calibration_downloads_new_file(mock_download, mock_get_session, tmp_path):
    # Create a valid FITS-like buffer (minimal FITS header)
    fits_header = b'SIMPLE  =                    T' + b' ' * 50 + b'END' + b' ' * 77
    fits_data = fits_header + b'\x00' * (2880 - len(fits_header))
    mock_download.return_value = io.BytesIO(fits_data)

    mock_session = mock.MagicMock()
    mock_cal = FakeCalibrationImage(filename='new_cal.fits')
    mock_session.query.return_value.get.return_value = mock_cal
    mock_get_session.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

    worker = make_worker(tmp_path)
    cal_info = CachedCalibrationInfo(
        id=1, filename='new_cal.fits', type='BIAS',
        frameid=12345, instrument_id=1, attributes={}
    )

    with mock.patch.object(worker, 'validate_fits', return_value=True):
        worker.download_calibration(cal_info)

    # Verify download was called with correct arguments
    mock_download.assert_called_once()
    call_args = mock_download.call_args
    # Verify file_info dict with frameid and filename
    file_info = call_args[0][0]
    assert file_info == {'frameid': 12345, 'filename': 'new_cal.fits'}
    # Verify runtime_context is passed
    assert call_args[0][1] == worker.runtime_context
    # Verify is_raw_frame=False for calibration files
    assert call_args[1]['is_raw_frame'] is False

    # File should exist
    assert (tmp_path / 'new_cal.fits').exists()


@mock.patch('banzai.utils.fits_utils.download_from_s3')
def test_download_calibration_raises_on_frame_not_available(mock_download, tmp_path):
    """Test that FrameNotAvailableError propagates correctly."""
    mock_download.side_effect = FrameNotAvailableError("Frame not found in archive")

    worker = make_worker(tmp_path)
    cal_info = CachedCalibrationInfo(
        id=1, filename='missing_frame.fits', type='BIAS',
        frameid=99999, instrument_id=1, attributes={}
    )

    with pytest.raises(FrameNotAvailableError):
        worker.download_calibration(cal_info)

    # Verify no file was created
    assert not (tmp_path / 'missing_frame.fits').exists()
    assert not (tmp_path / 'missing_frame.fits.tmp').exists()


@mock.patch('banzai.utils.fits_utils.download_from_s3')
def test_download_calibration_cleans_up_temp_on_validation_failure(mock_download, tmp_path):
    """Test temp file removed when FITS validation fails."""
    # Return invalid (non-FITS) data
    mock_download.return_value = io.BytesIO(b'not a valid fits file content')

    worker = make_worker(tmp_path)
    cal_info = CachedCalibrationInfo(
        id=1, filename='invalid_cal.fits', type='BIAS',
        frameid=12345, instrument_id=1, attributes={}
    )

    # Should raise ValueError for invalid FITS
    with pytest.raises(ValueError, match="Invalid FITS file"):
        worker.download_calibration(cal_info)

    # Verify temp file was cleaned up
    assert not (tmp_path / 'invalid_cal.fits.tmp').exists()
    # Final file should not exist either
    assert not (tmp_path / 'invalid_cal.fits').exists()


@mock.patch('banzai.utils.fits_utils.download_from_s3')
def test_download_calibration_cleans_up_temp_on_exception(mock_download, tmp_path):
    """Test temp file cleanup on unexpected exception."""
    # Return valid-looking data
    fits_header = b'SIMPLE  =                    T' + b' ' * 50 + b'END' + b' ' * 77
    fits_data = fits_header + b'\x00' * (2880 - len(fits_header))
    mock_download.return_value = io.BytesIO(fits_data)

    worker = make_worker(tmp_path)
    cal_info = CachedCalibrationInfo(
        id=1, filename='fail_rename.fits', type='BIAS',
        frameid=12345, instrument_id=1, attributes={}
    )

    # Mock os.rename to raise an exception after validation passes
    with mock.patch.object(worker, 'validate_fits', return_value=True):
        with mock.patch('os.rename', side_effect=OSError("Permission denied")):
            with pytest.raises(OSError, match="Permission denied"):
                worker.download_calibration(cal_info)

    # Verify temp file was cleaned up
    assert not (tmp_path / 'fail_rename.fits.tmp').exists()


# --- delete_calibration tests ---

@mock.patch('banzai.dbs.get_session')
def test_delete_calibration_removes_file(mock_get_session, tmp_path):
    # Create a file to delete
    file_to_delete = tmp_path / 'to_delete.fits'
    file_to_delete.touch()
    assert file_to_delete.exists()

    mock_session = mock.MagicMock()
    mock_cal = FakeCalibrationImage(filename='to_delete.fits')
    mock_session.query.return_value.filter_by.return_value.first.return_value = mock_cal
    mock_get_session.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

    worker = make_worker(tmp_path)
    worker.delete_calibration('to_delete.fits')

    assert not file_to_delete.exists()


@mock.patch('banzai.dbs.get_session')
def test_delete_calibration_clears_database_filepath(mock_get_session, tmp_path):
    # Create a file to delete
    file_to_delete = tmp_path / 'to_delete.fits'
    file_to_delete.touch()

    mock_session = mock.MagicMock()
    mock_cal = FakeCalibrationImage(filename='to_delete.fits', filepath=str(tmp_path))
    mock_session.query.return_value.filter_by.return_value.first.return_value = mock_cal
    mock_get_session.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

    worker = make_worker(tmp_path)
    worker.delete_calibration('to_delete.fits')

    # Verify filepath was cleared
    assert mock_cal.filepath is None
    mock_session.commit.assert_called()

    # Verify SET LOCAL application_name was executed to bypass trigger
    execute_calls = mock_session.execute.call_args_list
    assert len(execute_calls) >= 1
    sql_call = execute_calls[0]
    # The first argument is the text() SQL statement
    sql_text = str(sql_call[0][0])
    assert "SET LOCAL application_name = 'banzai_download_worker'" in sql_text


@mock.patch('banzai.dbs.get_session')
def test_delete_calibration_handles_missing_file(mock_get_session, tmp_path):
    mock_session = mock.MagicMock()
    mock_cal = FakeCalibrationImage(filename='nonexistent.fits')
    mock_session.query.return_value.filter_by.return_value.first.return_value = mock_cal
    mock_get_session.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

    worker = make_worker(tmp_path)
    # Should not raise even if file doesn't exist
    worker.delete_calibration('nonexistent.fits')

    # Should still clear database
    assert mock_cal.filepath is None


# --- validate_fits tests ---

def test_validate_fits_returns_true_for_valid_fits(tmp_path):
    # Create a minimal valid FITS file
    from astropy.io import fits
    import numpy as np

    fits_path = tmp_path / 'valid.fits'
    hdu = fits.PrimaryHDU(np.zeros((10, 10)))
    hdu.writeto(fits_path)

    worker = make_worker(tmp_path)
    result = worker.validate_fits(str(fits_path))

    assert result is True


def test_validate_fits_returns_false_for_corrupted_file(tmp_path):
    # Create a corrupted file
    corrupted_path = tmp_path / 'corrupted.fits'
    corrupted_path.write_bytes(b'not a valid fits file content')

    worker = make_worker(tmp_path)
    result = worker.validate_fits(str(corrupted_path))

    assert result is False


def test_validate_fits_returns_false_for_nonexistent_file(tmp_path):
    worker = make_worker(tmp_path)
    result = worker.validate_fits(str(tmp_path / 'nonexistent.fits'))

    assert result is False


# --- safe_to_delete tests ---

@mock.patch('banzai.dbs.get_session')
def test_safe_to_delete_returns_true_when_two_or_more_files_exist(mock_get_session, tmp_path):
    mock_session = mock.MagicMock()
    # The file to delete
    cal_to_delete = FakeCalibrationImage(
        id=1, filename='old_bias.fits', type='BIAS',
        instrument_id=1, attributes={'configuration_mode': 'default', 'binning': '1x1'}
    )
    mock_session.query.return_value.filter_by.return_value.first.return_value = cal_to_delete
    mock_get_session.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

    # Two other files for the same config that are already on disk
    needed_cals = {
        'bias1.fits': CachedCalibrationInfo(
            id=2, filename='bias1.fits', type='BIAS',
            frameid=100, instrument_id=1,
            attributes={'configuration_mode': 'default', 'binning': '1x1'}
        ),
        'bias2.fits': CachedCalibrationInfo(
            id=3, filename='bias2.fits', type='BIAS',
            frameid=101, instrument_id=1,
            attributes={'configuration_mode': 'default', 'binning': '1x1'}
        ),
    }
    files_on_disk = {'bias1.fits', 'bias2.fits', 'old_bias.fits'}

    worker = make_worker(tmp_path)
    result = worker.safe_to_delete('old_bias.fits', needed_cals, files_on_disk)

    assert result is True


@mock.patch('banzai.dbs.get_session')
def test_safe_to_delete_returns_false_when_only_one_file_exists(mock_get_session, tmp_path):
    mock_session = mock.MagicMock()
    cal_to_delete = FakeCalibrationImage(
        id=1, filename='old_bias.fits', type='BIAS',
        instrument_id=1, attributes={'configuration_mode': 'default', 'binning': '1x1'}
    )
    mock_session.query.return_value.filter_by.return_value.first.return_value = cal_to_delete
    mock_get_session.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

    # Only one other file for the same config on disk
    needed_cals = {
        'bias1.fits': CachedCalibrationInfo(
            id=2, filename='bias1.fits', type='BIAS',
            frameid=100, instrument_id=1,
            attributes={'configuration_mode': 'default', 'binning': '1x1'}
        ),
    }
    files_on_disk = {'bias1.fits', 'old_bias.fits'}

    worker = make_worker(tmp_path)
    result = worker.safe_to_delete('old_bias.fits', needed_cals, files_on_disk)

    assert result is False


@mock.patch('banzai.dbs.get_session')
def test_safe_to_delete_returns_false_when_cal_not_found(mock_get_session, tmp_path):
    mock_session = mock.MagicMock()
    mock_session.query.return_value.filter_by.return_value.first.return_value = None
    mock_get_session.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

    worker = make_worker(tmp_path)
    result = worker.safe_to_delete('unknown.fits', {}, set())

    assert result is False


@mock.patch('banzai.dbs.get_session')
def test_safe_to_delete_considers_filter_for_flat_type(mock_get_session, tmp_path):
    mock_session = mock.MagicMock()
    cal_to_delete = FakeCalibrationImage(
        id=1, filename='old_flat.fits', type='FLAT',
        instrument_id=1, attributes={'configuration_mode': 'default', 'binning': '1x1', 'filter': 'V'}
    )
    mock_session.query.return_value.filter_by.return_value.first.return_value = cal_to_delete
    mock_get_session.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

    # Two files for different filter - should not count
    needed_cals = {
        'flat1_r.fits': CachedCalibrationInfo(
            id=2, filename='flat1_r.fits', type='FLAT',
            frameid=100, instrument_id=1,
            attributes={'configuration_mode': 'default', 'binning': '1x1', 'filter': 'R'}
        ),
        'flat2_r.fits': CachedCalibrationInfo(
            id=3, filename='flat2_r.fits', type='FLAT',
            frameid=101, instrument_id=1,
            attributes={'configuration_mode': 'default', 'binning': '1x1', 'filter': 'R'}
        ),
    }
    files_on_disk = {'flat1_r.fits', 'flat2_r.fits', 'old_flat.fits'}

    worker = make_worker(tmp_path)
    result = worker.safe_to_delete('old_flat.fits', needed_cals, files_on_disk)

    # Should be False because files are different filter
    assert result is False


@mock.patch('banzai.dbs.get_session')
def test_safe_to_delete_handles_null_attributes(mock_get_session, tmp_path):
    """Test safe_to_delete when CalibrationImage.attributes is None.

    When a calibration has null attributes (can happen with legacy data or
    partial replication), safe_to_delete should return False as a safe default
    rather than crashing with AttributeError.
    """
    mock_session = mock.MagicMock()
    # Create a calibration with None attributes (can happen with legacy data)
    cal_to_delete = FakeCalibrationImage(
        id=1, filename='old_bias.fits', type='BIAS',
        instrument_id=1, attributes=None
    )
    # Override the default attributes value set in __init__
    cal_to_delete.attributes = None
    mock_session.query.return_value.filter_by.return_value.first.return_value = cal_to_delete
    mock_get_session.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

    # Needed cals have proper attributes
    needed_cals = {
        'bias1.fits': CachedCalibrationInfo(
            id=2, filename='bias1.fits', type='BIAS',
            frameid=100, instrument_id=1,
            attributes={'configuration_mode': 'default', 'binning': '1x1'}
        ),
        'bias2.fits': CachedCalibrationInfo(
            id=3, filename='bias2.fits', type='BIAS',
            frameid=101, instrument_id=1,
            attributes={'configuration_mode': 'default', 'binning': '1x1'}
        ),
    }
    files_on_disk = {'bias1.fits', 'bias2.fits', 'old_bias.fits'}

    worker = make_worker(tmp_path)

    # Should return False (safe default) when attributes are null
    result = worker.safe_to_delete('old_bias.fits', needed_cals, files_on_disk)
    assert result is False


# --- CachedCalibrationInfo tests ---

def test_cached_calibration_info_creation():
    info = CachedCalibrationInfo(
        id=1, filename='test.fits', type='BIAS',
        frameid=12345, instrument_id=1, attributes={'key': 'value'}
    )
    assert info.id == 1
    assert info.filename == 'test.fits'
    assert info.type == 'BIAS'
    assert info.frameid == 12345
    assert info.instrument_id == 1
    assert info.attributes == {'key': 'value'}


def test_cached_calibration_info_with_none_frameid():
    info = CachedCalibrationInfo(
        id=1, filename='test.fits', type='BIAS',
        frameid=None, instrument_id=1, attributes={}
    )
    assert info.frameid is None


# --- log_calibration_summary tests ---

@mock.patch('banzai.cache.download_worker.logger')
def test_log_calibration_summary_handles_empty_dict(mock_logger, tmp_path):
    worker = make_worker(tmp_path)
    # Reset the mock to clear the __init__ log call
    mock_logger.reset_mock()
    worker.log_calibration_summary({})

    # Should not log anything for empty dict (early return)
    mock_logger.info.assert_not_called()


@mock.patch('banzai.cache.download_worker.logger')
def test_log_calibration_summary_logs_type_counts(mock_logger, tmp_path):
    worker = make_worker(tmp_path)
    needed_cals = {
        'bias1.fits': CachedCalibrationInfo(
            id=1, filename='bias1.fits', type='BIAS',
            frameid=100, instrument_id=1, attributes={}
        ),
        'bias2.fits': CachedCalibrationInfo(
            id=2, filename='bias2.fits', type='BIAS',
            frameid=101, instrument_id=1, attributes={}
        ),
        'dark1.fits': CachedCalibrationInfo(
            id=3, filename='dark1.fits', type='DARK',
            frameid=102, instrument_id=1, attributes={}
        ),
    }
    worker.log_calibration_summary(needed_cals, log_details=False)

    # Verify logger.info was called with summary information
    info_calls = [str(call) for call in mock_logger.info.call_args_list]
    # Should log total count
    assert any('3 total files' in call for call in info_calls)
    # Should log BIAS count (2 files)
    assert any('BIAS' in call and '2' in call for call in info_calls)
    # Should log DARK count (1 file)
    assert any('DARK' in call and '1' in call for call in info_calls)


@mock.patch('banzai.cache.download_worker.logger')
def test_log_calibration_summary_logs_details_when_requested(mock_logger, tmp_path):
    worker = make_worker(tmp_path)
    needed_cals = {
        'bias1.fits': CachedCalibrationInfo(
            id=1, filename='bias1.fits', type='BIAS',
            frameid=100, instrument_id=1,
            attributes={'configuration_mode': 'default', 'binning': '1x1'}
        ),
        'flat1.fits': CachedCalibrationInfo(
            id=2, filename='flat1.fits', type='FLAT',
            frameid=101, instrument_id=1,
            attributes={'configuration_mode': 'default', 'binning': '1x1', 'filter': 'V'}
        ),
    }
    worker.log_calibration_summary(needed_cals, log_details=True)

    # Verify detailed breakdown is logged
    info_calls = [str(call) for call in mock_logger.info.call_args_list]
    # Should log "Configuration breakdown:" when log_details=True
    assert any('Configuration breakdown' in call for call in info_calls)
    # Should include configuration details
    assert any('BIAS' in call for call in info_calls)
    assert any('FLAT' in call for call in info_calls)


# --- _get_type_summary tests ---

def test_get_type_summary_returns_empty_for_no_cals(tmp_path):
    worker = make_worker(tmp_path)
    result = worker._get_type_summary({})
    assert result == "empty"


def test_get_type_summary_returns_formatted_string(tmp_path):
    worker = make_worker(tmp_path)
    needed_cals = {
        'bias1.fits': CachedCalibrationInfo(
            id=1, filename='bias1.fits', type='BIAS',
            frameid=100, instrument_id=1, attributes={}
        ),
        'dark1.fits': CachedCalibrationInfo(
            id=2, filename='dark1.fits', type='DARK',
            frameid=101, instrument_id=1, attributes={}
        ),
    }
    result = worker._get_type_summary(needed_cals)
    assert 'BIAS:1' in result
    assert 'DARK:1' in result


# --- reconcile_orphaned_filepaths tests ---

@mock.patch('banzai.dbs.get_session')
def test_reconcile_orphaned_filepaths_clears_orphaned(mock_get_session, tmp_path):
    mock_session = mock.MagicMock()
    # Calibration with filepath set but not in needed set
    orphaned_cal = FakeCalibrationImage(
        filename='orphaned.fits', filepath=str(tmp_path)
    )
    mock_session.query.return_value.filter.return_value.all.return_value = [orphaned_cal]
    mock_get_session.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

    worker = make_worker(tmp_path)
    needed_filenames = {'needed1.fits', 'needed2.fits'}
    worker.reconcile_orphaned_filepaths(needed_filenames)

    assert orphaned_cal.filepath is None
    mock_session.commit.assert_called()

    # Verify SET LOCAL application_name was executed to bypass trigger
    execute_calls = mock_session.execute.call_args_list
    assert len(execute_calls) >= 1
    sql_call = execute_calls[0]
    sql_text = str(sql_call[0][0])
    assert "SET LOCAL application_name = 'banzai_download_worker'" in sql_text


@mock.patch('banzai.dbs.get_session')
def test_reconcile_orphaned_filepaths_keeps_needed(mock_get_session, tmp_path):
    mock_session = mock.MagicMock()
    # Calibration with filepath set and in needed set
    needed_cal = FakeCalibrationImage(
        filename='needed.fits', filepath=str(tmp_path)
    )
    mock_session.query.return_value.filter.return_value.all.return_value = [needed_cal]
    mock_get_session.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
    mock_get_session.return_value.__exit__ = mock.MagicMock(return_value=False)

    worker = make_worker(tmp_path)
    needed_filenames = {'needed.fits'}
    worker.reconcile_orphaned_filepaths(needed_filenames)

    # Filepath should not be cleared
    assert needed_cal.filepath == str(tmp_path)


# --- run() method tests ---

def test_run_downloads_missing_and_deletes_outdated(tmp_path):
    """Test that run() orchestrates download/delete correctly."""
    worker = make_worker(tmp_path)

    # Track call count to exit after first iteration
    call_count = [0]
    def exit_after_first(seconds):
        call_count[0] += 1
        if call_count[0] >= 1:
            raise KeyboardInterrupt()

    needed_cal = CachedCalibrationInfo(
        id=1, filename='needed.fits', type='BIAS',
        frameid=12345, instrument_id=1, attributes={}
    )

    with mock.patch.object(worker, 'get_calibrations_to_cache', return_value={'needed.fits': needed_cal}):
        with mock.patch.object(worker, 'get_cached_files', return_value={'outdated.fits'}):
            with mock.patch.object(worker, 'safe_to_delete', return_value=True):
                with mock.patch.object(worker, 'download_calibration') as mock_download:
                    with mock.patch.object(worker, 'delete_calibration') as mock_delete:
                        with mock.patch.object(worker, 'reconcile_orphaned_filepaths'):
                            with mock.patch('time.sleep', side_effect=exit_after_first):
                                worker.run()

    # Verify download was called for the needed file
    mock_download.assert_called_once_with(needed_cal)
    # Verify delete was called for the outdated file
    mock_delete.assert_called_once_with('outdated.fits')


@mock.patch('banzai.cache.download_worker.logger')
def test_run_logs_startup_state(mock_logger, tmp_path):
    """Test that startup logs initial cache state."""
    worker = make_worker(tmp_path)
    # Reset mock to clear __init__ log
    mock_logger.reset_mock()

    call_count = [0]
    def exit_after_first(seconds):
        call_count[0] += 1
        if call_count[0] >= 1:
            raise KeyboardInterrupt()

    needed_cal = CachedCalibrationInfo(
        id=1, filename='bias.fits', type='BIAS',
        frameid=100, instrument_id=1, attributes={}
    )

    with mock.patch.object(worker, 'get_calibrations_to_cache', return_value={'bias.fits': needed_cal}):
        with mock.patch.object(worker, 'get_cached_files', return_value={'bias.fits'}):
            with mock.patch.object(worker, 'reconcile_orphaned_filepaths'):
                with mock.patch('time.sleep', side_effect=exit_after_first):
                    worker.run()

    # Verify log_calibration_summary was called (via logger.info with summary)
    info_calls = [str(call) for call in mock_logger.info.call_args_list]
    # Should log initial cache state on startup
    assert any('Initial cache state' in call or 'cache' in call.lower() for call in info_calls)


def test_run_backs_off_on_error(tmp_path):
    """Test that errors in loop cause 30s backoff."""
    worker = make_worker(tmp_path)

    call_count = [0]
    def track_sleep(seconds):
        call_count[0] += 1
        if call_count[0] >= 2:
            raise KeyboardInterrupt()

    error_count = [0]
    def raise_then_succeed():
        error_count[0] += 1
        if error_count[0] == 1:
            raise Exception("Database connection failed")
        return {}

    with mock.patch.object(worker, 'get_calibrations_to_cache', side_effect=raise_then_succeed):
        with mock.patch.object(worker, 'get_cached_files', return_value=set()):
            with mock.patch.object(worker, 'reconcile_orphaned_filepaths'):
                with mock.patch('time.sleep', side_effect=track_sleep) as mock_sleep:
                    worker.run()

    # Verify time.sleep was called with 30 seconds for backoff
    sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
    assert 30 in sleep_calls


# --- Integration test with real SQLite database ---

def test_get_calibrations_to_cache_ranking_with_real_database(tmp_path):
    """
    Integration test verifying the ranking logic in get_calibrations_to_cache().

    Uses a real SQLite database to test that the ranking SQL properly selects
    the top 2 calibrations per configuration based on dateobs.
    """
    from datetime import datetime, timedelta
    from banzai import dbs
    from banzai.cache.download_worker import DownloadWorker, CachedCalibrationInfo

    # Create database file path
    db_path = tmp_path / 'test_cache.db'
    db_address = f'sqlite:///{db_path}'

    # Create tables using banzai's create_db
    dbs.create_db(db_address)

    # Set up test data
    with dbs.get_session(db_address) as session:
        # Create site
        site = dbs.Site(
            id='tst',
            timezone=-7,
            latitude=30.0,
            longitude=-110.0,
            elevation=2000.0
        )
        session.add(site)
        session.flush()

        # Create instrument
        instrument = dbs.Instrument(
            id=1,
            site='tst',
            camera='fa01',
            type='1m0-SciCam-Sinistro',
            name='fa01',
            nx=4096,
            ny=4096
        )
        session.add(instrument)
        session.flush()

        # Create cache config
        cache_config = dbs.CacheConfig(
            site_id='tst',
            instrument_types=['*'],
            cache_root=str(tmp_path / 'cache')
        )
        session.add(cache_config)
        session.flush()

        # Create base datetime for dateobs
        base_date = datetime(2024, 1, 15, 12, 0, 0)

        # Create 3 BIAS calibrations (should return 2 newest)
        for i in range(3):
            bias = dbs.CalibrationImage(
                type='BIAS',
                filename=f'bias_{i}.fits',
                filepath=None,
                frameid=1000 + i,
                dateobs=base_date - timedelta(days=i),  # newer to older
                datecreated=base_date - timedelta(days=i),
                instrument_id=1,
                is_master=True,
                is_bad=False,
                attributes={
                    'configuration_mode': 'default',
                    'binning': '1x1'
                }
            )
            session.add(bias)

        # Create 2 DARK calibrations (should return both)
        for i in range(2):
            dark = dbs.CalibrationImage(
                type='DARK',
                filename=f'dark_{i}.fits',
                filepath=None,
                frameid=2000 + i,
                dateobs=base_date - timedelta(days=i),
                datecreated=base_date - timedelta(days=i),
                instrument_id=1,
                is_master=True,
                is_bad=False,
                attributes={
                    'configuration_mode': 'default',
                    'binning': '1x1'
                }
            )
            session.add(dark)

        # Create 2 SKYFLAT with filter='V' (should return both)
        for i in range(2):
            skyflat_v = dbs.CalibrationImage(
                type='SKYFLAT',
                filename=f'skyflat_v_{i}.fits',
                filepath=None,
                frameid=3000 + i,
                dateobs=base_date - timedelta(days=i),
                datecreated=base_date - timedelta(days=i),
                instrument_id=1,
                is_master=True,
                is_bad=False,
                attributes={
                    'configuration_mode': 'default',
                    'binning': '1x1',
                    'filter': 'V'
                }
            )
            session.add(skyflat_v)

        # Create 1 SKYFLAT with filter='R' (should return the 1)
        skyflat_r = dbs.CalibrationImage(
            type='SKYFLAT',
            filename='skyflat_r_0.fits',
            filepath=None,
            frameid=4000,
            dateobs=base_date,
            datecreated=base_date,
            instrument_id=1,
            is_master=True,
            is_bad=False,
            attributes={
                'configuration_mode': 'default',
                'binning': '1x1',
                'filter': 'R'
            }
        )
        session.add(skyflat_r)

        session.commit()

    # Create worker and call get_calibrations_to_cache
    cache_root = str(tmp_path / 'cache')
    worker = DownloadWorker(
        db_address=db_address,
        cache_root=cache_root,
        runtime_context=FakeContext()
    )

    result = worker.get_calibrations_to_cache()

    # Verify all returned objects are CachedCalibrationInfo instances
    for filename, cal_info in result.items():
        assert isinstance(cal_info, CachedCalibrationInfo), \
            f"Expected CachedCalibrationInfo, got {type(cal_info)}"

    # Verify total count: 2 BIAS + 2 DARK + 2 SKYFLAT(V) + 1 SKYFLAT(R) = 7
    assert len(result) == 7, f"Expected 7 total calibrations, got {len(result)}"

    # Verify BIAS: should have the 2 newest (bias_0 and bias_1)
    bias_cals = [k for k in result.keys() if k.startswith('bias_')]
    assert len(bias_cals) == 2, f"Expected 2 BIAS calibrations, got {len(bias_cals)}"
    assert 'bias_0.fits' in result, "Expected newest BIAS (bias_0.fits)"
    assert 'bias_1.fits' in result, "Expected second newest BIAS (bias_1.fits)"
    assert 'bias_2.fits' not in result, "Oldest BIAS (bias_2.fits) should be excluded"

    # Verify DARK: should have both (dark_0 and dark_1)
    dark_cals = [k for k in result.keys() if k.startswith('dark_')]
    assert len(dark_cals) == 2, f"Expected 2 DARK calibrations, got {len(dark_cals)}"
    assert 'dark_0.fits' in result
    assert 'dark_1.fits' in result

    # Verify SKYFLAT with filter='V': should have both (skyflat_v_0 and skyflat_v_1)
    skyflat_v_cals = [k for k in result.keys() if k.startswith('skyflat_v_')]
    assert len(skyflat_v_cals) == 2, f"Expected 2 SKYFLAT(V) calibrations, got {len(skyflat_v_cals)}"
    assert 'skyflat_v_0.fits' in result
    assert 'skyflat_v_1.fits' in result

    # Verify SKYFLAT with filter='R': should have the 1 that exists
    skyflat_r_cals = [k for k in result.keys() if k.startswith('skyflat_r_')]
    assert len(skyflat_r_cals) == 1, f"Expected 1 SKYFLAT(R) calibration, got {len(skyflat_r_cals)}"
    assert 'skyflat_r_0.fits' in result

    # Verify CachedCalibrationInfo attributes are correct for a sample
    bias_0 = result['bias_0.fits']
    assert bias_0.filename == 'bias_0.fits'
    assert bias_0.type == 'BIAS'
    assert bias_0.frameid == 1000
    assert bias_0.instrument_id == 1
    assert bias_0.attributes['configuration_mode'] == 'default'
    assert bias_0.attributes['binning'] == '1x1'
