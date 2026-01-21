import io
import os
from unittest import mock

import pytest

from banzai.cache.download_worker import DownloadWorker, CachedCalibrationInfo
from banzai.tests.utils import FakeContext

pytestmark = pytest.mark.download_worker


class FakeCacheConfig:
    """Fake cache configuration for testing."""
    def __init__(self, site_id='cpt', instrument_types=None):
        self.site_id = site_id
        self.instrument_types = instrument_types or ['*']
        self.cache_root = '/data/calibrations'


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

    # Verify download was called
    mock_download.assert_called_once()
    # File should exist
    assert (tmp_path / 'new_cal.fits').exists()


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

def test_log_calibration_summary_handles_empty_dict(tmp_path):
    worker = make_worker(tmp_path)
    # Should not raise
    worker.log_calibration_summary({})


def test_log_calibration_summary_logs_type_counts(tmp_path):
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
    # Should not raise
    worker.log_calibration_summary(needed_cals, log_details=True)


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
