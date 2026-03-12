import io
import os
from datetime import datetime
from unittest import mock

import numpy as np
import pytest
from astropy.io import fits

from banzai import dbs, settings
from banzai.cache.download_worker import (
    get_calibrations_to_cache, get_cache_path, download_calibration,
    delete_calibration, run_download_worker_daemon,
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
         mock.patch('banzai.cache.download_worker._update_filepath') as up:
        download_calibration('sqlite:///test.db', processed_path, FakeContext(), cal)

    dl.assert_not_called()
    up.assert_called_once_with('sqlite:///test.db', 1, dest_dir)


def test_skips_null_frameid(tmp_path):
    cal = _make_cal(frameid=None)
    with mock.patch('banzai.utils.fits_utils.download_from_s3') as dl:
        download_calibration('sqlite:///test.db', str(tmp_path), FakeContext(), cal)
    dl.assert_not_called()


def test_raises_on_invalid_fits(tmp_path):
    cal = _make_cal(filename='bad.fits')
    with mock.patch('banzai.utils.fits_utils.download_from_s3', return_value=io.BytesIO(b'bad')):
        with pytest.raises(OSError):
            download_calibration('sqlite:///test.db', str(tmp_path), FakeContext(), cal)


# --- delete_calibration tests ---

def test_delete_clears_db_when_file_missing():
    cal = mock.MagicMock(id=1, filename='gone.fits', filepath='/nonexistent')
    with mock.patch('banzai.cache.download_worker._update_filepath') as up:
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


def test_returns_top_2_per_config(db_address, tmp_path):
    inst_id = _seed_db(db_address)
    bias_attrs = _attrs_for_type('BIAS', configuration_mode='default', binning='1x1')
    with dbs.get_session(db_address) as session:
        _add_cal(session, inst_id, 'BIAS', 'old.fits', 1, datetime(2024, 1, 1), bias_attrs)
        _add_cal(session, inst_id, 'BIAS', 'mid.fits', 2, datetime(2024, 1, 2), bias_attrs)
        _add_cal(session, inst_id, 'BIAS', 'new.fits', 3, datetime(2024, 1, 3), bias_attrs)

    filenames = {r.filename for r in get_calibrations_to_cache(db_address, 'tst', ['*'])}
    assert filenames == {'mid.fits', 'new.fits'}


def test_partitions_independently_by_config(db_address, tmp_path):
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


def test_filters_by_instrument_type(db_address, tmp_path):
    sinistro_id = _seed_db(db_address, camera='fa01', inst_type='1m0-SciCam-Sinistro')
    floyds_id = _seed_db(db_address, camera='en01', inst_type='2m0-FLOYDS-SciCam')
    bias_attrs = _attrs_for_type('BIAS', configuration_mode='default', binning='1x1')
    with dbs.get_session(db_address) as session:
        _add_cal(session, sinistro_id, 'BIAS', 'sinistro.fits', 1, datetime(2024, 1, 1), bias_attrs)
        _add_cal(session, floyds_id, 'BIAS', 'floyds.fits', 2, datetime(2024, 1, 1), bias_attrs)

    filenames = {r.filename for r in
                 get_calibrations_to_cache(db_address, 'tst', ['1m0-SciCam-Sinistro'])}
    assert filenames == {'sinistro.fits'}


def test_wildcard_returns_all_instrument_types(db_address, tmp_path):
    sinistro_id = _seed_db(db_address, camera='fa01', inst_type='1m0-SciCam-Sinistro')
    floyds_id = _seed_db(db_address, camera='en01', inst_type='2m0-FLOYDS-SciCam')
    bias_attrs = _attrs_for_type('BIAS', configuration_mode='default', binning='1x1')
    with dbs.get_session(db_address) as session:
        _add_cal(session, sinistro_id, 'BIAS', 'sinistro.fits', 1, datetime(2024, 1, 1), bias_attrs)
        _add_cal(session, floyds_id, 'BIAS', 'floyds.fits', 2, datetime(2024, 1, 1), bias_attrs)

    filenames = {r.filename for r in get_calibrations_to_cache(db_address, 'tst', ['*'])}
    assert filenames == {'sinistro.fits', 'floyds.fits'}


def test_biases_ignore_filter(db_address, tmp_path):
    """Biases taken with different filters should be grouped together."""
    inst_id = _seed_db(db_address)
    bias_attrs = _attrs_for_type('BIAS', configuration_mode='default', binning='1x1')
    with dbs.get_session(db_address) as session:
        _add_cal(session, inst_id, 'BIAS', 'bias_V.fits', 1, datetime(2024, 1, 1), bias_attrs)
        _add_cal(session, inst_id, 'BIAS', 'bias_B.fits', 2, datetime(2024, 1, 2), bias_attrs)
        _add_cal(session, inst_id, 'BIAS', 'bias_R.fits', 3, datetime(2024, 1, 3), bias_attrs)

    filenames = {r.filename for r in get_calibrations_to_cache(db_address, 'tst', ['*'])}
    assert filenames == {'bias_B.fits', 'bias_R.fits'}


def test_darks_partitioned_by_temperature(db_address, tmp_path):
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


def test_skyflats_partitioned_by_filter(db_address, tmp_path):
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
        download_calibration(db_address, processed_path, FakeContext(), cal)

    dl.assert_called_once()
    assert dl.call_args[0][0] == {'frameid': 123, 'filename': 'bias.fits'}
    assert dl.call_args[1]['is_raw_frame'] is False

    expected_path = get_cache_path(processed_path, cal)
    assert os.path.exists(os.path.join(expected_path, 'bias.fits'))
    with dbs.get_session(db_address) as session:
        assert session.query(dbs.CalibrationImage).get(cal_id).filepath == expected_path


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
