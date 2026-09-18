import io
import os
from datetime import datetime
from unittest import mock

import numpy as np
import pytest
from astropy.io import fits
from psycopg.errors import UndefinedTable
from sqlalchemy.exc import ProgrammingError

from banzai import dbs, settings
from banzai.cache.download_worker import (
    get_calibrations_to_cache, get_cache_path, download_calibration,
    delete_calibration, run_download_worker, run_download_worker_daemon, _site_has_calibrations,
)
from banzai.tests.utils import FakeContext

pytestmark = pytest.mark.download_worker


def _make_fits_buffer():
    buf = io.BytesIO()
    hdu = fits.PrimaryHDU(np.zeros((2, 2), dtype=np.float32))
    hdu.writeto(buf)
    buf.seek(0)
    return buf


def _make_cal(filename='bias.fits', frameid=123, **overrides):
    defaults = dict(id=1, filename=filename, frameid=frameid,
                    dateobs=datetime(2024, 1, 15), site='tst', camera='fa01')
    defaults.update(overrides)
    return mock.MagicMock(**defaults)


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

def test_skips_when_file_exists(tmp_path):
    cal = _make_cal()
    processed_path = str(tmp_path)
    dest_dir = get_cache_path(processed_path, cal)
    os.makedirs(dest_dir, exist_ok=True)
    open(os.path.join(dest_dir, 'bias.fits'), 'w').close()

    with mock.patch('banzai.utils.fits_utils.download_from_s3') as dl, \
         mock.patch('banzai.cache.download_worker.update_filepath') as up:
        result = download_calibration('sqlite:///test.db', processed_path, FakeContext(), cal)

    assert result is None
    dl.assert_not_called()
    up.assert_called_once_with('sqlite:///test.db', 1, dest_dir)


def test_skips_null_frameid(tmp_path):
    cal = _make_cal(frameid=None)
    with mock.patch('banzai.utils.fits_utils.download_from_s3') as dl:
        result = download_calibration('sqlite:///test.db', str(tmp_path), FakeContext(), cal)
    assert result is None
    dl.assert_not_called()


def test_raises_on_invalid_fits(tmp_path):
    cal = _make_cal(filename='bad.fits')
    with mock.patch('banzai.utils.fits_utils.download_from_s3', return_value=io.BytesIO(b'bad')):
        with pytest.raises(OSError):
            download_calibration('sqlite:///test.db', str(tmp_path), FakeContext(), cal)


# --- delete_calibration tests ---

def test_delete_clears_db_when_file_missing():
    cal = mock.MagicMock(id=1, filename='gone.fits', filepath='/nonexistent')
    with mock.patch('banzai.cache.download_worker.update_filepath') as up:
        delete_calibration('sqlite:///test.db', cal)
    up.assert_called_once_with('sqlite:///test.db', 1, None)


def test_delete_happy_path_with_real_db(db_address, tmp_path):
    inst_id = _seed_db(db_address)
    with dbs.get_session(db_address) as session:
        _add_cal(session, inst_id, 'BIAS', 'del.fits', 100, datetime(2024, 1, 1),
                 {'configuration_mode': 'default', 'binning': '1x1'})
    with dbs.get_session(db_address) as session:
        cal = session.query(dbs.CalibrationImage).filter_by(filename='del.fits').first()
        cal.filepath = str(tmp_path)
        cal_id = cal.id

    open(os.path.join(str(tmp_path), 'del.fits'), 'w').close()

    row = mock.MagicMock(id=cal_id, filename='del.fits', filepath=str(tmp_path))
    delete_calibration(db_address, row)

    assert not os.path.exists(os.path.join(str(tmp_path), 'del.fits'))
    with dbs.get_session(db_address) as session:
        cal = session.query(dbs.CalibrationImage).get(cal_id)
        assert cal.filepath is None


# --- get_calibrations_to_cache tests ---

def _attrs_for_type(cal_type, **overrides):
    attrs = {k: '' for k in settings.CALIBRATION_SET_CRITERIA.get(cal_type, [])}
    attrs.update(overrides)
    return attrs


def test_returns_top_2_per_config(db_address):
    inst_id = _seed_db(db_address)
    bias_attrs = _attrs_for_type('BIAS', configuration_mode='default', binning='1x1')
    with dbs.get_session(db_address) as session:
        _add_cal(session, inst_id, 'BIAS', 'old.fits', 1, datetime(2024, 1, 1), bias_attrs)
        _add_cal(session, inst_id, 'BIAS', 'mid.fits', 2, datetime(2024, 1, 2), bias_attrs)
        _add_cal(session, inst_id, 'BIAS', 'new.fits', 3, datetime(2024, 1, 3), bias_attrs)

    filenames = {r.filename for r in get_calibrations_to_cache(db_address, 'tst', ['*'])}
    assert filenames == {'mid.fits', 'new.fits'}


def test_partitions_independently_by_config(db_address):
    inst_id = _seed_db(db_address)
    with dbs.get_session(db_address) as session:
        for i, binning in enumerate(['1x1', '2x2']):
            for j in range(3):
                attrs = _attrs_for_type('BIAS', configuration_mode='default', binning=binning)
                _add_cal(session, inst_id, 'BIAS', f'bias_{binning}_{j}.fits',
                         i * 10 + j, datetime(2024, 1, j + 1), attrs)

    filenames = {r.filename for r in get_calibrations_to_cache(db_address, 'tst', ['*'])}
    assert filenames == {'bias_1x1_1.fits', 'bias_1x1_2.fits',
                         'bias_2x2_1.fits', 'bias_2x2_2.fits'}


def test_filters_by_instrument_type(db_address):
    sinistro_id = _seed_db(db_address, camera='fa01', inst_type='1m0-SciCam-Sinistro')
    floyds_id = _seed_db(db_address, camera='en01', inst_type='2m0-FLOYDS-SciCam')
    bias_attrs = _attrs_for_type('BIAS', configuration_mode='default', binning='1x1')
    with dbs.get_session(db_address) as session:
        _add_cal(session, sinistro_id, 'BIAS', 'sinistro.fits', 1, datetime(2024, 1, 1), bias_attrs)
        _add_cal(session, floyds_id, 'BIAS', 'floyds.fits', 2, datetime(2024, 1, 1), bias_attrs)

    filenames = {r.filename for r in
                 get_calibrations_to_cache(db_address, 'tst', ['1m0-SciCam-Sinistro'])}
    assert filenames == {'sinistro.fits'}


def test_wildcard_returns_all_instrument_types(db_address):
    sinistro_id = _seed_db(db_address, camera='fa01', inst_type='1m0-SciCam-Sinistro')
    floyds_id = _seed_db(db_address, camera='en01', inst_type='2m0-FLOYDS-SciCam')
    bias_attrs = _attrs_for_type('BIAS', configuration_mode='default', binning='1x1')
    with dbs.get_session(db_address) as session:
        _add_cal(session, sinistro_id, 'BIAS', 'sinistro.fits', 1, datetime(2024, 1, 1), bias_attrs)
        _add_cal(session, floyds_id, 'BIAS', 'floyds.fits', 2, datetime(2024, 1, 1), bias_attrs)

    filenames = {r.filename for r in get_calibrations_to_cache(db_address, 'tst', ['*'])}
    assert filenames == {'sinistro.fits', 'floyds.fits'}


def test_biases_ignore_filter(db_address):
    """Biases taken with different filters should be grouped together."""
    inst_id = _seed_db(db_address)
    bias_attrs = _attrs_for_type('BIAS', configuration_mode='default', binning='1x1')
    with dbs.get_session(db_address) as session:
        _add_cal(session, inst_id, 'BIAS', 'bias_V.fits', 1, datetime(2024, 1, 1), bias_attrs)
        _add_cal(session, inst_id, 'BIAS', 'bias_B.fits', 2, datetime(2024, 1, 2), bias_attrs)
        _add_cal(session, inst_id, 'BIAS', 'bias_R.fits', 3, datetime(2024, 1, 3), bias_attrs)

    filenames = {r.filename for r in get_calibrations_to_cache(db_address, 'tst', ['*'])}
    assert filenames == {'bias_B.fits', 'bias_R.fits'}


def test_darks_partitioned_by_temperature(db_address):
    inst_id = _seed_db(db_address)
    with dbs.get_session(db_address) as session:
        for i, temp in enumerate(['5', '10']):
            for j in range(3):
                _add_cal(session, inst_id, 'DARK', f'dark_t{temp}_{j}.fits',
                         i * 10 + j, datetime(2024, 1, j + 1),
                         _attrs_for_type('DARK', configuration_mode='default', binning='1x1',
                                         ccd_temperature=temp))

    filenames = {r.filename for r in get_calibrations_to_cache(db_address, 'tst', ['*'])}
    assert filenames == {'dark_t5_1.fits', 'dark_t5_2.fits',
                         'dark_t10_1.fits', 'dark_t10_2.fits'}


def test_get_calibrations_to_cache_excludes_null_frameid(db_address):
    inst_id = _seed_db(db_address)
    bias_attrs = _attrs_for_type('BIAS', configuration_mode='default', binning='1x1')
    with dbs.get_session(db_address) as session:
        _add_cal(session, inst_id, 'BIAS', 'has_frameid.fits', 1, datetime(2024, 1, 1), bias_attrs)
        _add_cal(session, inst_id, 'BIAS', 'null_frameid.fits', None, datetime(2024, 1, 2), bias_attrs)

    filenames = {r.filename for r in get_calibrations_to_cache(db_address, 'tst', ['*'])}
    assert filenames == {'has_frameid.fits'}


def test_skyflats_partitioned_by_filter(db_address):
    inst_id = _seed_db(db_address)
    with dbs.get_session(db_address) as session:
        for i, filt in enumerate(['V', 'B']):
            for j in range(3):
                _add_cal(session, inst_id, 'SKYFLAT', f'flat_{filt}_{j}.fits',
                         i * 10 + j, datetime(2024, 1, j + 1),
                         _attrs_for_type('SKYFLAT', configuration_mode='default', binning='1x1',
                                         filter=filt))

    filenames = {r.filename for r in get_calibrations_to_cache(db_address, 'tst', ['*'])}
    assert filenames == {'flat_V_1.fits', 'flat_V_2.fits',
                         'flat_B_1.fits', 'flat_B_2.fits'}


# --- download integration test ---

def test_download_happy_path_with_real_db(db_address, tmp_path):
    inst_id = _seed_db(db_address)
    with dbs.get_session(db_address) as session:
        _add_cal(session, inst_id, 'BIAS', 'bias.fits', 123, datetime(2024, 1, 15),
                 _attrs_for_type('BIAS', configuration_mode='default', binning='1x1'))
    with dbs.get_session(db_address) as session:
        cal_id = session.query(dbs.CalibrationImage).filter_by(filename='bias.fits').first().id

    processed_path = str(tmp_path)
    cal = _make_cal(id=cal_id)
    with mock.patch('banzai.utils.fits_utils.download_from_s3', return_value=_make_fits_buffer()) as dl:
        result = download_calibration(db_address, processed_path, FakeContext(), cal)

    assert result is None
    dl.assert_called_once()
    assert dl.call_args[0][0] == {'frameid': 123, 'filename': 'bias.fits'}
    assert dl.call_args[1]['is_raw_frame'] is False

    expected_path = get_cache_path(processed_path, cal)
    assert os.path.exists(os.path.join(expected_path, 'bias.fits'))
    with dbs.get_session(db_address) as session:
        assert session.query(dbs.CalibrationImage).get(cal_id).filepath == expected_path


# --- worker loop tests ---

@pytest.mark.parametrize('lookup_failure, download_failure, startup_waits', [
    (None, None, 0),
    (None, None, 2),
    (RuntimeError('Archive unavailable'), OSError('Download failed'), 0),
])
def test_worker_recovers_ids_after_cache_work(db_address, tmp_path, caplog, lookup_failure, download_failure,
                                             startup_waits):
    inst_id = _seed_db(db_address)
    contents = _make_fits_buffer().getvalue()
    older_path = tmp_path / 'tst/fa01/20240102/processed/older.fits'
    older_path.parent.mkdir(parents=True)
    older_path.write_bytes(contents)
    known_path = tmp_path / 'tst/fa01/20240103/processed/known.fits'
    with dbs.get_session(db_address) as session:
        for day, filename, frameid in [(1, 'historical.fits', None), (2, 'older.fits', 1),
                                       (3, 'known.fits', 2), (4, 'missing.fits', None),
                                       (5, 'unavailable.fits', None)]:
            _add_cal(session, inst_id, 'BIAS', filename, frameid, datetime(2024, 1, day), _attrs_for_type('BIAS'))
        session.flush()
        session.query(dbs.CalibrationImage).filter_by(filename='older.fits').update(
            {'filepath': str(older_path.parent)})

    lookup_passes = []

    def find_frame(filename, *args):
        lookup_passes.append(sleep.call_count - startup_waits)
        assert known_path.exists()  # Normal downloads must finish before any archive lookups.
        if filename == 'missing.fits':
            return 42
        if lookup_failure:
            raise lookup_failure
        return None

    pending_startup_waits = startup_waits

    def site_has_calibrations(*args):
        nonlocal pending_startup_waits
        if pending_startup_waits:
            pending_startup_waits -= 1
            raise ProgrammingError(None, None, UndefinedTable('calimages is not initialized'))
        return _site_has_calibrations(*args)

    download_results = [io.BytesIO(contents), download_failure or io.BytesIO(contents)]
    with mock.patch('banzai.utils.fits_utils.basename_search_in_archive', side_effect=find_frame) as lookup, \
         mock.patch('banzai.utils.fits_utils.download_from_s3', side_effect=download_results) as download, \
         mock.patch('banzai.cache.download_worker._site_has_calibrations', side_effect=site_has_calibrations), \
         mock.patch('banzai.cache.download_worker.time.monotonic', return_value=0), \
         mock.patch('banzai.cache.download_worker.time.sleep',
                    side_effect=[None] * (startup_waits + 3) + [KeyboardInterrupt]) as sleep:
        with pytest.raises(KeyboardInterrupt):
            run_download_worker(db_address, 'tst', ['*'], str(tmp_path), FakeContext())

    assert caplog.text.count('Waiting for database tables to be initialized') == bool(startup_waits)
    assert 'Error in worker loop' not in caplog.text
    assert [call.args[0] for call in lookup.call_args_list] == ['unavailable.fits', 'missing.fits']
    assert lookup_passes == [0, 1 if lookup_failure else 0]
    assert [call.args[0] for call in download.call_args_list] == [
        {'frameid': 2, 'filename': 'known.fits'}, {'frameid': 42, 'filename': 'missing.fits'}]
    # A failed replacement must not evict the older file, even on the following cooldown pass.
    assert older_path.exists() == (download_failure is not None)
    assert known_path.exists()
    recovered_path = tmp_path / 'tst/fa01/20240104/processed/missing.fits'
    assert recovered_path.exists() == (download_failure is None)
    with dbs.get_session(db_address) as session:
        recovered = session.query(dbs.CalibrationImage).filter_by(filename='missing.fits').one()
        assert recovered.frameid == 42
        assert recovered.filepath == (None if download_failure else str(recovered_path.parent))
        assert session.query(dbs.CalibrationImage).filter_by(filename='unavailable.fits').one().frameid is None


# --- run_download_worker_daemon tests ---

@pytest.mark.parametrize('types_arg,expected', [
    ('*', ['*']),
    ('1m0-SciCam-Sinistro', ['1m0-SciCam-Sinistro']),
    ('1m0-SciCam-Sinistro,2m0-FLOYDS-SciCam', ['1m0-SciCam-Sinistro', '2m0-FLOYDS-SciCam']),
    ('1m0-SciCam-Sinistro, 2m0-FLOYDS-SciCam', ['1m0-SciCam-Sinistro', '2m0-FLOYDS-SciCam']),
])
def test_daemon_parses_instrument_types(types_arg, expected):
    argv = ['banzai_download_worker', '--db-address=sqlite:///test.db',
            '--site-id=tst', f'--instrument-types={types_arg}']
    with mock.patch('sys.argv', argv), \
         mock.patch('banzai.cache.download_worker.run_download_worker') as run:
        run.side_effect = SystemExit(0)
        with pytest.raises(SystemExit):
            run_download_worker_daemon()
    assert run.call_args[0][2] == expected
