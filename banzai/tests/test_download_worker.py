import io
import os
import time
from datetime import date, datetime, timedelta
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


# --- get_calibrations_to_cache: real SQLite integration tests ---

def test_top2_ranking(db_address, tmp_path):
    inst_id = _seed_db(db_address)
    base = datetime(2024, 1, 15)
    with dbs.get_session(db_address) as session:
        for i in range(3):
            _add_cal(session, inst_id, 'BIAS', f'bias_{i}.fits', 1000 + i,
                     base - timedelta(days=i), {'configuration_mode': 'default', 'binning': '1x1'})
    worker = DownloadWorker(db_address, 'tst', ['*'], str(tmp_path), FakeContext())
    result = worker.get_calibrations_to_cache()
    filenames = {r.filename for r in result}
    assert 'bias_0.fits' in filenames
    assert 'bias_1.fits' in filenames
    assert 'bias_2.fits' not in filenames


def test_filter_partitions_separately(db_address, tmp_path):
    inst_id = _seed_db(db_address)
    base = datetime(2024, 1, 15)
    with dbs.get_session(db_address) as session:
        for i in range(3):
            _add_cal(session, inst_id, 'SKYFLAT', f'flat_v_{i}.fits', 2000 + i,
                     base - timedelta(days=i), {'configuration_mode': 'default', 'binning': '1x1', 'filter': 'V'})
            _add_cal(session, inst_id, 'SKYFLAT', f'flat_r_{i}.fits', 3000 + i,
                     base - timedelta(days=i), {'configuration_mode': 'default', 'binning': '1x1', 'filter': 'R'})
    worker = DownloadWorker(db_address, 'tst', ['*'], str(tmp_path), FakeContext())
    result = worker.get_calibrations_to_cache()
    filenames = {r.filename for r in result}
    # 2 per filter
    assert len([f for f in filenames if 'flat_v_' in f]) == 2
    assert len([f for f in filenames if 'flat_r_' in f]) == 2


def test_filters_by_site(db_address, tmp_path):
    inst_id = _seed_db(db_address, site_id='tst')
    _seed_db(db_address, site_id='ogg', camera='fa02')
    base = datetime(2024, 1, 15)
    with dbs.get_session(db_address) as session:
        _add_cal(session, inst_id, 'BIAS', 'tst_bias.fits', 100, base,
                 {'configuration_mode': 'default', 'binning': '1x1'})
        # instrument_id=2 belongs to ogg
        _add_cal(session, 2, 'BIAS', 'ogg_bias.fits', 200, base,
                 {'configuration_mode': 'default', 'binning': '1x1'})
    worker = DownloadWorker(db_address, 'tst', ['*'], str(tmp_path), FakeContext())
    result = worker.get_calibrations_to_cache()
    filenames = {r.filename for r in result}
    assert 'tst_bias.fits' in filenames
    assert 'ogg_bias.fits' not in filenames


def test_filters_by_instrument_type(db_address, tmp_path):
    _seed_db(db_address, camera='fa01', inst_type='1m0-SciCam-Sinistro')
    _seed_db(db_address, camera='kb01', inst_type='0m4-SciCam-SBIG')
    base = datetime(2024, 1, 15)
    with dbs.get_session(db_address) as session:
        # inst 1 = Sinistro, inst 2 = SBIG (but same site from _seed_db reuse - we inserted same site twice)
        _add_cal(session, 1, 'BIAS', 'sin_bias.fits', 100, base,
                 {'configuration_mode': 'default', 'binning': '1x1'})
        _add_cal(session, 2, 'BIAS', 'sbig_bias.fits', 200, base,
                 {'configuration_mode': 'default', 'binning': '1x1'})
    worker = DownloadWorker(db_address, 'tst', ['1m0-SciCam-Sinistro'], str(tmp_path), FakeContext())
    result = worker.get_calibrations_to_cache()
    filenames = {r.filename for r in result}
    assert 'sin_bias.fits' in filenames
    assert 'sbig_bias.fits' not in filenames


def test_wildcard_instrument_types_returns_all(db_address, tmp_path):
    _seed_db(db_address, camera='fa01', inst_type='1m0-SciCam-Sinistro')
    _seed_db(db_address, camera='kb01', inst_type='0m4-SciCam-SBIG')
    base = datetime(2024, 1, 15)
    with dbs.get_session(db_address) as session:
        _add_cal(session, 1, 'BIAS', 'sin_bias.fits', 100, base,
                 {'configuration_mode': 'default', 'binning': '1x1'})
        _add_cal(session, 2, 'BIAS', 'sbig_bias.fits', 200, base,
                 {'configuration_mode': 'default', 'binning': '1x1'})
    worker = DownloadWorker(db_address, 'tst', ['*'], str(tmp_path), FakeContext())
    result = worker.get_calibrations_to_cache()
    filenames = {r.filename for r in result}
    assert 'sin_bias.fits' in filenames
    assert 'sbig_bias.fits' in filenames


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


class _StopLoop(BaseException):
    """Non-Exception BaseException to break out of run() loop cleanly in tests."""
    pass


# --- run() tests ---

def test_run_downloads_and_deletes(worker, tmp_path):
    needed = mock.MagicMock(filename='needed.fits')
    stale = mock.MagicMock(filename='stale.fits', filepath=str(tmp_path))

    with mock.patch.object(worker, 'get_calibrations_to_cache', return_value=[needed]), \
         mock.patch.object(worker, 'get_cache_path', return_value='/nonexistent'), \
         mock.patch('banzai.dbs.get_session') as mock_gs, \
         mock.patch.object(worker, 'download_calibration') as dl, \
         mock.patch.object(worker, 'delete_calibration') as rm, \
         mock.patch('time.sleep', side_effect=_StopLoop):
        mock_session = mock.MagicMock()
        mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = [stale]
        mock_gs.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
        mock_gs.return_value.__exit__ = mock.MagicMock(return_value=False)
        with pytest.raises(_StopLoop):
            worker.run()

    dl.assert_called_once_with(needed)
    rm.assert_called_once_with(stale)


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


@mock.patch('banzai.cache.download_worker.HEARTBEAT_INTERVAL', 0)
def test_run_logs_heartbeat(worker):
    with mock.patch.object(worker, 'get_calibrations_to_cache', return_value=[]), \
         mock.patch('banzai.dbs.get_session') as mock_gs, \
         mock.patch('banzai.cache.download_worker.logger') as mock_logger, \
         mock.patch('time.sleep', side_effect=_StopLoop):
        mock_session = mock.MagicMock()
        mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = []
        mock_gs.return_value.__enter__ = mock.MagicMock(return_value=mock_session)
        mock_gs.return_value.__exit__ = mock.MagicMock(return_value=False)
        with pytest.raises(_StopLoop):
            worker.run()

    heartbeat_calls = [c for c in mock_logger.info.call_args_list if 'healthy' in str(c)]
    assert len(heartbeat_calls) >= 1


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
