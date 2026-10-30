import importlib.util
import io
import os

import mock
import pytest
import argparse

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, inspect, text
from astropy.io import fits

import banzai.main
from banzai import dbs
from banzai.bpm import BadPixelMaskLoader
from banzai.lco import LCOCalibrationFrame
from banzai.tests.utils import FakeCCDData, FakeInstrument, FakeLCOObservationFrame, FakeResponse
from astropy.utils.data import get_pkg_data_filename

pytestmark = pytest.mark.dbs


@mock.patch('banzai.dbs.requests.get',
            return_value=FakeResponse(get_pkg_data_filename('data/configdb_example.json', 'banzai.tests')))
@mock.patch('argparse.ArgumentParser.parse_args',
            return_value=argparse.Namespace(log_level='debug', db_address='sqlite:///test.db'))
def setup_module(mock_argparse, mockrequests):
    banzai.main.create_db()


def teardown_module():
    os.remove('test.db')


def test_add_or_update():
    with dbs.get_session(db_address='sqlite:///test.db') as db_session:
        # Add a fake instrument
        dbs.add_or_update_record(db_session, dbs.Instrument,
                                 equivalence_criteria={'site': 'bpl', 'camera': 'kb101'},
                                 record_attributes={'site': 'bpl', 'camera': 'kb101', 'type': 'SBig', 'name': 'kb101'})
        db_session.commit()

        # Make sure it got added
        query = db_session.query(dbs.Instrument).filter(dbs.Instrument.site == 'bpl')
        instrument = query.filter(dbs.Instrument.camera == 'kb101').first()
        assert instrument is not None

        # Update the fake instrument's name
        dbs.add_or_update_record(db_session, dbs.Instrument,
                                 equivalence_criteria={'site': 'bpl', 'camera': 'kb101'},
                                 record_attributes={'site': 'bpl', 'camera': 'kb101',
                                                    'type': 'SBig', 'name': 'foo'})

        db_session.commit()
        # Make sure the update took
        query = db_session.query(dbs.Instrument).filter(dbs.Instrument.site == 'bpl')
        instrument = query.filter(dbs.Instrument.name == 'foo').first()

        # make sure there is still only one new instrument in the table
        query = db_session.query(dbs.Instrument).filter(dbs.Instrument.site == 'bpl')
        instruments = query.filter(dbs.Instrument.camera == 'kb101').all()
        assert len(instruments) == 1

        # Clean up for other methods
        db_session.delete(instrument)
        db_session.commit()


@mock.patch('banzai.main.archive_get')
@mock.patch('banzai.lco.LCOFrameFactory.open')
def test_add_bpms_from_archive_persists_frameid(mock_open, mock_archive_get, monkeypatch):
    bpm = LCOCalibrationFrame([FakeCCDData(meta={'OBSTYPE': 'BPM',
                                                'DATE-OBS': '2021-04-20T00:00:00.000',
                                                'DATE': '2021-04-20T00:00:00.000'})], 'bpm.fits')
    bpm.instrument = FakeInstrument()
    mock_open.return_value = bpm
    mock_archive_get.return_value.json.return_value = {'results': [{'id': 1234, 'filename': bpm.filename}]}
    monkeypatch.setattr('sys.argv', ['banzai_populate_bpms', '--db-address', 'sqlite:///test.db'])

    banzai.main.add_bpms_from_archive()

    with dbs.get_session('sqlite:///test.db') as session:
        saved = session.query(dbs.CalibrationImage).filter_by(filename=bpm.filename).one()
        assert saved.frameid == 1234


@pytest.mark.parametrize('upload_to_archive, frame_id', [(True, 5678), (False, None)])
@mock.patch('banzai.utils.file_utils.post_to_ingester', return_value={'frameid': 5678})
@mock.patch('banzai.lco.LCOFrameFactory.open')
def test_add_super_calibration_persists_frameid(mock_open, mock_ingester, upload_to_archive, frame_id,
                                               tmp_path, monkeypatch):
    filepath = tmp_path / f'super-bias-{upload_to_archive}.fits'
    filepath.touch()
    calibration = LCOCalibrationFrame([FakeCCDData(meta={'OBSTYPE': 'BIAS',
                                                        'DATE-OBS': '2021-04-20T00:00:00.000',
                                                        'DATE': '2021-04-20T00:00:00.000'})], str(filepath))
    calibration.instrument = FakeInstrument()
    mock_open.return_value = calibration
    argv = ['banzai_add_super_calibration', str(filepath), '--db-address', 'sqlite:///test.db']
    if upload_to_archive:
        argv.append('--upload-to-archive')
    monkeypatch.setattr('sys.argv', argv)

    banzai.main.add_super_calibration()

    with dbs.get_session('sqlite:///test.db') as session:
        saved = session.query(dbs.CalibrationImage).filter_by(filename=filepath.name).one()
        assert saved.frameid == frame_id
    assert mock_ingester.call_count == int(upload_to_archive)


@pytest.mark.parametrize('separate_cal_db', [False, True], ids=['main-db', 'separate-cal-db'])
@pytest.mark.parametrize('frame_id, local_file', [(None, False), (1234, False), (None, True)],
                         ids=['recover-id', 'known-id', 'local-only'])
@mock.patch('banzai.dbs.update_calibration_frameid', wraps=dbs.update_calibration_frameid)
@mock.patch('banzai.lco.LCOFrameFactory.get_instrument_from_header')
@mock.patch('banzai.utils.fits_utils.basename_search_in_archive', return_value=1234)
@mock.patch('banzai.utils.fits_utils.download_from_s3')
def test_calibration_user_frameid_persistence(mock_download, mock_basename, mock_instrument, mock_update,
                                             frame_id, local_file, separate_cal_db, tmp_path, monkeypatch):
    db_address = f'sqlite:///{tmp_path}/main.db'
    cal_db_address = f'sqlite:///{tmp_path}/calibration.db' if separate_cal_db else db_address
    monkeypatch.setenv('DB_ADDRESS', db_address)
    monkeypatch.delenv('CAL_DB_ADDRESS', raising=False)
    if separate_cal_db:
        monkeypatch.setenv('CAL_DB_ADDRESS', cal_db_address)
    context = banzai.main.parse_args(banzai.main.settings, parse_system_args=False)
    assert context.cal_db_address == cal_db_address

    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(nx=2, ny=2,
                                                        meta={'DATE-OBS': '2021-04-20T00:00:00.000',
                                                              'CCDSUM': '1 1'})])
    mock_instrument.return_value = image.instrument
    filename = 'recovered-bpm.fits'
    buffer = io.BytesIO()
    fits.PrimaryHDU(data=image.mask, header=fits.Header({'OBSTYPE': 'BPM', 'SATURATE': 65535})).writeto(buffer)
    if local_file:
        (tmp_path / filename).write_bytes(buffer.getvalue())
    buffer.seek(0)
    mock_download.return_value = buffer

    for address in {db_address, cal_db_address}:
        dbs.create_db(address)
        with dbs.get_session(address) as session:
            session.add(dbs.CalibrationImage(filename=filename, type='BPM',
                                             frameid=frame_id if address == cal_db_address else 9876,
                                             filepath=str(tmp_path) if local_file else None,
                                             dateobs=image.dateobs, instrument_id=image.instrument.id,
                                             is_master=True, is_bad=False,
                                             attributes={'configuration_mode': image.configuration_mode,
                                                         'binning': str(image.binning)}))

    assert BadPixelMaskLoader(context).do_stage(image) is image

    for address in {db_address, cal_db_address}:
        expected_frame_id = (None if local_file else 1234) if address == cal_db_address else 9876
        with dbs.get_session(address) as session:
            saved = session.query(dbs.CalibrationImage).filter_by(filename=filename).one()
            assert saved.frameid == expected_frame_id
    recovered = frame_id is None and not local_file
    assert mock_basename.call_count == int(recovered)
    assert mock_download.call_count == int(not local_file)
    assert mock_update.call_count == int(recovered)


def test_create_db_default_does_not_create_site_tables(tmp_path):
    addr = f'sqlite:///{tmp_path}/aws_only.db'
    dbs.create_db(addr)
    engine = create_engine(addr)
    assert not inspect(engine).has_table('stacks')
    assert not inspect(engine).has_table('stackframes')


def test_create_db_site_deploy_true_creates_site_tables(tmp_path):
    addr = f'sqlite:///{tmp_path}/site.db'
    dbs.create_db(addr, site_deploy=True)
    engine = create_engine(addr)
    inspector = inspect(engine)
    assert inspector.has_table('stacks')
    assert inspector.has_table('stackframes')
    stackframe_columns = {column['name']: column for column in inspector.get_columns('stackframes')}
    assert stackframe_columns['moluid']['nullable'] is False
    assert stackframe_columns['stack_num']['nullable'] is False
    assert stackframe_columns['molnum']['nullable'] is False
    assert inspector.get_pk_constraint('stacks')['constrained_columns'] == ['moluid', 'molnum']


def test_get_session_site_deploy_true_raises_when_site_tables_missing(tmp_path):
    addr = f'sqlite:///{tmp_path}/aws_only.db'
    dbs.create_db(addr)
    with pytest.raises(RuntimeError, match='stacks'):
        with dbs.get_session(addr, site_deploy=True):
            pass


def test_get_session_site_deploy_true_succeeds_with_site_tables(tmp_path):
    addr = f'sqlite:///{tmp_path}/site.db'
    dbs.create_db(addr, site_deploy=True)
    with dbs.get_session(addr, site_deploy=True) as session:
        assert session is not None


def _load_molnum_migration():
    path = os.path.join(os.path.dirname(dbs.__file__), 'alembic', 'versions',
                        'c7d2e8a41f60_key_stacks_on_moluid_molnum.py')
    spec = importlib.util.spec_from_file_location('molnum_migration', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_migration(engine, migration_step):
    with engine.begin() as connection:
        with Operations.context(MigrationContext.configure(connection)):
            migration_step()


def test_molnum_migration_upgrades_old_schema_and_keeps_rows(tmp_path):
    engine = create_engine(f'sqlite:///{tmp_path}/site.db')
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE stacks (moluid VARCHAR(100) PRIMARY KEY, camera VARCHAR(50), "
                                "frmtotal INTEGER, status VARCHAR(20), created_at DATETIME, "
                                "last_stackframe_at DATETIME, completed_at DATETIME, finalize_attempts INTEGER, "
                                "next_attempt_at DATETIME, last_preview_count INTEGER)"))
        connection.execute(text("CREATE TABLE stackframes (id INTEGER PRIMARY KEY, moluid VARCHAR(100) NOT NULL, "
                                "stack_num INTEGER NOT NULL, filepath VARCHAR(255) NOT NULL, dateobs DATETIME, "
                                "is_last BOOLEAN, created_at DATETIME, UNIQUE (moluid, stack_num))"))
        connection.execute(text("INSERT INTO stacks (moluid, camera, status) VALUES ('mol-old', 'cam1', 'timeout')"))
        connection.execute(text("INSERT INTO stackframes (moluid, stack_num, filepath) "
                                "VALUES ('mol-old', 1, '/data/old.fits')"))

    _run_migration(engine, _load_molnum_migration().upgrade)

    inspector = inspect(engine)
    assert inspector.get_pk_constraint('stacks')['constrained_columns'] == ['moluid', 'molnum']
    with engine.connect() as connection:
        assert connection.execute(text("SELECT moluid, molnum, status FROM stacks")).one() == ('mol-old', 0, 'timeout')
        assert connection.execute(text("SELECT moluid, molnum, stack_num FROM stackframes")).one() == ('mol-old', 0, 1)


def test_molnum_migration_skips_tables_already_on_new_schema(tmp_path):
    addr = f'sqlite:///{tmp_path}/site.db'
    dbs.create_db(addr, site_deploy=True)
    with dbs.get_session(addr, site_deploy=True) as session:
        session.add(dbs.Stack(moluid='mol-new', molnum=5, camera='cam1', status='active'))

    _run_migration(create_engine(addr), _load_molnum_migration().upgrade)

    with dbs.get_session(addr, site_deploy=True) as session:
        assert session.query(dbs.Stack.moluid, dbs.Stack.molnum).one() == ('mol-new', 5)


def test_molnum_migration_downgrade_restores_moluid_key(tmp_path):
    addr = f'sqlite:///{tmp_path}/site.db'
    dbs.create_db(addr, site_deploy=True)
    with dbs.get_session(addr, site_deploy=True) as session:
        session.add(dbs.Stack(moluid='mol-rgb', molnum=81, camera='cam1', status='active'))
        session.add(dbs.Stack(moluid='mol-rgb', molnum=82, camera='cam1', status='active'))
    engine = create_engine(addr)

    _run_migration(engine, _load_molnum_migration().downgrade)

    inspector = inspect(engine)
    assert inspector.get_pk_constraint('stacks')['constrained_columns'] == ['moluid']
    assert 'molnum' not in {column['name'] for column in inspector.get_columns('stackframes')}
    with engine.connect() as connection:
        assert connection.execute(text("SELECT moluid FROM stacks")).all() == [('mol-rgb',)]
