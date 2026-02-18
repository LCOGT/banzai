import io
import os
import time
from datetime import date, datetime
from unittest import mock

import pytest

from banzai import dbs
from banzai.cache.download_worker import DownloadWorker, run_download_worker_daemon
from banzai.tests.utils import FakeContext

pytestmark = pytest.mark.download_worker


@pytest.fixture
def worker(tmp_path):
    return DownloadWorker(
        db_address='sqlite:///test.db',
        site_id='tst',
        instrument_types=['*'],
        processed_path=str(tmp_path),
        runtime_context=FakeContext(),
    )


@pytest.fixture
def db_address(tmp_path):
    addr = f'sqlite:///{tmp_path}/test.db'
    dbs.create_db(addr)
    return addr


def _seed_db(db_address, site_id='tst', camera='fa01', inst_type='1m0-SciCam-Sinistro'):
    """Insert a site (if needed) + instrument. Returns instrument_id."""
    with dbs.get_session(db_address) as session:
        if not session.query(dbs.Site).get(site_id):
            session.add(dbs.Site(id=site_id, timezone=-7, latitude=30.0, longitude=-110.0, elevation=2000.0))
            session.flush()
        inst = dbs.Instrument(site=site_id, camera=camera, type=inst_type, name=camera, nx=4096, ny=4096)
        session.add(inst)
        session.flush()
        return inst.id


def _add_cal(session, instrument_id, cal_type, filename, frameid, dateobs, attrs):
    session.add(dbs.CalibrationImage(
        type=cal_type, filename=filename, filepath=None, frameid=frameid,
        dateobs=dateobs, datecreated=dateobs, instrument_id=instrument_id,
        is_master=True, is_bad=False, attributes=attrs,
    ))


# --- download_calibration tests ---

def test_download_new_file(worker, tmp_path):
    cal = mock.MagicMock(id=1, filename='bias.fits', frameid=123,
                         dateobs=datetime(2024, 1, 15), site='tst', camera='fa01')
    fits_data = io.BytesIO(b'\x00' * 2880)

    with mock.patch('banzai.utils.fits_utils.download_from_s3', return_value=fits_data) as dl, \
         mock.patch('banzai.utils.fits_utils.get_primary_header', return_value=mock.MagicMock()), \
         mock.patch.object(worker, '_update_filepath') as up:
        worker.download_calibration(cal)

    dl.assert_called_once()
    assert dl.call_args[0][0] == {'frameid': 123, 'filename': 'bias.fits'}
    assert dl.call_args[1]['is_raw_frame'] is False
    up.assert_called_once_with(1, worker.get_cache_path(cal))
    dest = os.path.join(worker.get_cache_path(cal), 'bias.fits')
    assert os.path.exists(dest)


def test_skips_when_file_exists(worker, tmp_path):
    cal = mock.MagicMock(id=1, filename='bias.fits', frameid=123,
                         dateobs=datetime(2024, 1, 15), site='tst', camera='fa01')
    dest_dir = worker.get_cache_path(cal)
    os.makedirs(dest_dir, exist_ok=True)
    open(os.path.join(dest_dir, 'bias.fits'), 'w').close()

    with mock.patch('banzai.utils.fits_utils.download_from_s3') as dl, \
         mock.patch.object(worker, '_update_filepath') as up:
        worker.download_calibration(cal)

    dl.assert_not_called()
    up.assert_called_once_with(1, dest_dir)


def test_skips_null_frameid(worker):
    cal = mock.MagicMock(id=1, filename='bias.fits', frameid=None,
                         dateobs=datetime(2024, 1, 15), site='tst', camera='fa01')
    with mock.patch('banzai.utils.fits_utils.download_from_s3') as dl:
        worker.download_calibration(cal)
    dl.assert_not_called()


def test_cleans_temp_on_validation_failure(worker, tmp_path):
    cal = mock.MagicMock(id=1, filename='bad.fits', frameid=123,
                         dateobs=datetime(2024, 1, 15), site='tst', camera='fa01')
    with mock.patch('banzai.utils.fits_utils.download_from_s3', return_value=io.BytesIO(b'bad')), \
         mock.patch('banzai.utils.fits_utils.get_primary_header', return_value=None):
        with pytest.raises(ValueError, match="Invalid FITS"):
            worker.download_calibration(cal)
    dest_dir = worker.get_cache_path(cal)
    assert not os.path.exists(os.path.join(dest_dir, 'bad.fits.tmp'))
    assert not os.path.exists(os.path.join(dest_dir, 'bad.fits'))


def test_cleans_temp_on_exception(worker, tmp_path):
    cal = mock.MagicMock(id=1, filename='fail.fits', frameid=123,
                         dateobs=datetime(2024, 1, 15), site='tst', camera='fa01')
    with mock.patch('banzai.utils.fits_utils.download_from_s3', return_value=io.BytesIO(b'\x00' * 100)), \
         mock.patch('banzai.utils.fits_utils.get_primary_header', return_value=mock.MagicMock()), \
         mock.patch('os.rename', side_effect=OSError("disk full")):
        with pytest.raises(OSError):
            worker.download_calibration(cal)
    dest_dir = worker.get_cache_path(cal)
    assert not os.path.exists(os.path.join(dest_dir, 'fail.fits.tmp'))


# --- delete_calibration tests ---

def test_delete_removes_file_and_clears_db(worker, tmp_path):
    test_dir = str(tmp_path / 'deltest')
    os.makedirs(test_dir)
    filepath = os.path.join(test_dir, 'old.fits')
    open(filepath, 'w').close()
    cal = mock.MagicMock(id=1, filename='old.fits', filepath=test_dir)

    with mock.patch.object(worker, '_update_filepath') as up:
        worker.delete_calibration(cal)
    assert not os.path.exists(filepath)
    up.assert_called_once_with(1, None)


def test_delete_clears_db_when_file_missing(worker):
    cal = mock.MagicMock(id=1, filename='gone.fits', filepath='/nonexistent')
    with mock.patch.object(worker, '_update_filepath') as up:
        worker.delete_calibration(cal)
    up.assert_called_once_with(1, None)


def test_delete_happy_path_with_real_db(db_address, tmp_path):
    inst_id = _seed_db(db_address)
    with dbs.get_session(db_address) as session:
        _add_cal(session, inst_id, 'BIAS', 'del.fits', 100, datetime(2024, 1, 1),
                 {'configuration_mode': 'default', 'binning': '1x1'})
    # Set filepath so delete can clear it
    with dbs.get_session(db_address) as session:
        cal = session.query(dbs.CalibrationImage).filter_by(filename='del.fits').first()
        cal.filepath = str(tmp_path)
        cal_id = cal.id

    # Create file on disk
    open(os.path.join(str(tmp_path), 'del.fits'), 'w').close()

    worker = DownloadWorker(db_address, 'tst', ['*'], str(tmp_path), FakeContext())
    row = mock.MagicMock(id=cal_id, filename='del.fits', filepath=str(tmp_path))
    worker.delete_calibration(row)

    assert not os.path.exists(os.path.join(str(tmp_path), 'del.fits'))
    with dbs.get_session(db_address) as session:
        cal = session.query(dbs.CalibrationImage).get(cal_id)
        assert cal.filepath is None


# --- get_calibrations_to_cache tests ---

def _default_attrs(binning='1x1', filter_name=''):
    return {'configuration_mode': 'default', 'binning': binning, 'filter': filter_name}


def test_returns_top_2_per_config(db_address, tmp_path):
    inst_id = _seed_db(db_address)
    with dbs.get_session(db_address) as session:
        _add_cal(session, inst_id, 'BIAS', 'old.fits', 1, datetime(2024, 1, 1), _default_attrs())
        _add_cal(session, inst_id, 'BIAS', 'mid.fits', 2, datetime(2024, 1, 2), _default_attrs())
        _add_cal(session, inst_id, 'BIAS', 'new.fits', 3, datetime(2024, 1, 3), _default_attrs())

    worker = DownloadWorker(db_address, 'tst', ['*'], str(tmp_path), FakeContext())
    filenames = {r.filename for r in worker.get_calibrations_to_cache()}
    assert filenames == {'mid.fits', 'new.fits'}


def test_partitions_independently_by_config(db_address, tmp_path):
    inst_id = _seed_db(db_address)
    with dbs.get_session(db_address) as session:
        for i, binning in enumerate(['1x1', '2x2']):
            for j in range(3):
                _add_cal(session, inst_id, 'BIAS', f'bias_{binning}_{j}.fits',
                         i * 10 + j, datetime(2024, 1, j + 1), _default_attrs(binning=binning))

    worker = DownloadWorker(db_address, 'tst', ['*'], str(tmp_path), FakeContext())
    filenames = {r.filename for r in worker.get_calibrations_to_cache()}
    # Top 2 from each partition (by dateobs desc): j=2 and j=1
    assert filenames == {'bias_1x1_1.fits', 'bias_1x1_2.fits',
                         'bias_2x2_1.fits', 'bias_2x2_2.fits'}


def test_filters_by_instrument_type(db_address, tmp_path):
    sinistro_id = _seed_db(db_address, camera='fa01', inst_type='1m0-SciCam-Sinistro')
    floyds_id = _seed_db(db_address, camera='en01', inst_type='2m0-FLOYDS-SciCam')
    with dbs.get_session(db_address) as session:
        _add_cal(session, sinistro_id, 'BIAS', 'sinistro.fits', 1, datetime(2024, 1, 1), _default_attrs())
        _add_cal(session, floyds_id, 'BIAS', 'floyds.fits', 2, datetime(2024, 1, 1), _default_attrs())

    worker = DownloadWorker(db_address, 'tst', ['1m0-SciCam-Sinistro'], str(tmp_path), FakeContext())
    filenames = {r.filename for r in worker.get_calibrations_to_cache()}
    assert filenames == {'sinistro.fits'}


def test_wildcard_returns_all_instrument_types(db_address, tmp_path):
    sinistro_id = _seed_db(db_address, camera='fa01', inst_type='1m0-SciCam-Sinistro')
    floyds_id = _seed_db(db_address, camera='en01', inst_type='2m0-FLOYDS-SciCam')
    with dbs.get_session(db_address) as session:
        _add_cal(session, sinistro_id, 'BIAS', 'sinistro.fits', 1, datetime(2024, 1, 1), _default_attrs())
        _add_cal(session, floyds_id, 'BIAS', 'floyds.fits', 2, datetime(2024, 1, 1), _default_attrs())

    worker = DownloadWorker(db_address, 'tst', ['*'], str(tmp_path), FakeContext())
    filenames = {r.filename for r in worker.get_calibrations_to_cache()}
    assert filenames == {'sinistro.fits', 'floyds.fits'}


# --- download integration test ---

def test_download_happy_path_with_real_db(db_address, tmp_path):
    inst_id = _seed_db(db_address)
    with dbs.get_session(db_address) as session:
        _add_cal(session, inst_id, 'BIAS', 'bias.fits', 123, datetime(2024, 1, 15), _default_attrs())
    with dbs.get_session(db_address) as session:
        cal_id = session.query(dbs.CalibrationImage).filter_by(filename='bias.fits').first().id

    worker = DownloadWorker(db_address, 'tst', ['*'], str(tmp_path), FakeContext())
    cal = mock.MagicMock(id=cal_id, filename='bias.fits', frameid=123,
                         dateobs=datetime(2024, 1, 15), site='tst', camera='fa01')
    with mock.patch('banzai.utils.fits_utils.download_from_s3', return_value=io.BytesIO(b'\x00' * 2880)), \
         mock.patch('banzai.utils.fits_utils.get_primary_header', return_value=mock.MagicMock()):
        worker.download_calibration(cal)

    expected_path = worker.get_cache_path(cal)
    assert os.path.exists(os.path.join(expected_path, 'bias.fits'))
    with dbs.get_session(db_address) as session:
        assert session.query(dbs.CalibrationImage).get(cal_id).filepath == expected_path


class _StopLoop(BaseException):
    """Non-Exception BaseException to break out of run() loop cleanly in tests."""
    pass


# --- run() tests ---

def test_run_backs_off_on_error(worker):
    call_count = [0]

    def sleep_side_effect(seconds):
        call_count[0] += 1
        if call_count[0] >= 2:
            raise _StopLoop()

    with mock.patch.object(worker, 'get_calibrations_to_cache', side_effect=Exception("db error")), \
         mock.patch('time.sleep', side_effect=sleep_side_effect) as mock_sleep:
        with pytest.raises(_StopLoop):
            worker.run()

    assert mock_sleep.call_args_list[0][0][0] == 30


# --- get_cache_path test ---

def test_get_cache_path(worker):
    cal = mock.MagicMock(dateobs=date(2024, 1, 15), site='tst', camera='fa01')
    path = worker.get_cache_path(cal)
    assert path.endswith(os.path.join('tst', 'fa01', '20240115', 'processed'))


# --- run_download_worker_daemon tests ---

def test_daemon_exits_without_db_address():
    with mock.patch.dict(os.environ, {}, clear=True):
        with pytest.raises(SystemExit) as exc:
            run_download_worker_daemon()
        assert exc.value.code == 1


def test_daemon_exits_without_site_id():
    with mock.patch.dict(os.environ, {'DB_ADDRESS': 'sqlite:///test.db'}, clear=True):
        with pytest.raises(SystemExit) as exc:
            run_download_worker_daemon()
        assert exc.value.code == 1
