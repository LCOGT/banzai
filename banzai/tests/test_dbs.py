import os

import mock
import pytest
import argparse

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
