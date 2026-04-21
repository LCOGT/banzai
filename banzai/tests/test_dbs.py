import os

import mock
import pytest
import argparse

from sqlalchemy import create_engine, inspect

import banzai.main
from banzai import dbs
from banzai.tests.utils import FakeResponse
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


def test_create_db_default_does_not_create_site_tables(tmp_path):
    addr = f'sqlite:///{tmp_path}/aws_only.db'
    dbs.create_db(addr)
    engine = create_engine(addr)
    assert not inspect(engine).has_table('stack_frames')


def test_create_db_site_true_creates_site_tables(tmp_path):
    addr = f'sqlite:///{tmp_path}/site.db'
    dbs.create_db(addr, site=True)
    engine = create_engine(addr)
    assert inspect(engine).has_table('stack_frames')


def test_get_session_site_true_raises_when_site_tables_missing(tmp_path):
    addr = f'sqlite:///{tmp_path}/aws_only.db'
    dbs.create_db(addr)
    with pytest.raises(RuntimeError, match='stack_frames'):
        with dbs.get_session(addr, site=True):
            pass


def test_get_session_site_true_succeeds_with_site_tables(tmp_path):
    addr = f'sqlite:///{tmp_path}/site.db'
    dbs.create_db(addr, site=True)
    with dbs.get_session(addr, site=True) as session:
        assert session is not None
