import os
import json

import mock
from astropy.utils.data import get_pkg_data_filename

from banzai import dbs


class FakeResponse(object):
    def __init__(self):
        with open(get_pkg_data_filename('data/configdb_example.json')) as f:
            self.data = json.load(f)

    def json(self):
        return self.data


@mock.patch('banzai.dbs.requests.get', return_value=FakeResponse())
def setup_module(mockrequests):
    dbs.create_db('.', db_address='sqlite:///test.db')


def teardown_module():
    os.remove('test.db')


def test_add_or_update():
    db_session = dbs.get_session(db_address='sqlite:///test.db')
    # Add a fake telescope
    dbs.add_or_update_record(db_session, dbs.Instrument, {'site': 'bpl', 'camera': 'kb101'},
                             {'site': 'bpl', 'camera': 'kb101', 'type': 'SBig',
                              'schedulable': False})
    db_session.commit()

    # Make sure it got added
    query = db_session.query(dbs.Instrument).filter(dbs.Instrument.site == 'bpl')
    telescope = query.filter(dbs.Instrument.camera == 'kb101').first()
    assert telescope is not None

    # Update the fake telescope
    dbs.add_or_update_record(db_session, dbs.Instrument, {'site': 'bpl', 'camera': 'kb101'},
                             {'site': 'bpl', 'camera': 'kb101', 'type': 'SBig',
                              'schedulable': True})

    db_session.commit()
    # Make sure the update took
    query = db_session.query(dbs.Instrument).filter(dbs.Instrument.site == 'bpl')
    telescope = query.filter(dbs.Instrument.camera == 'kb101').first()
    assert telescope is not None
    assert telescope.schedulable

    # make sure there is only one new telescope in the table
    query = db_session.query(dbs.Instrument).filter(dbs.Instrument.site == 'bpl')
    telescopes = query.filter(dbs.Instrument.camera == 'kb101').all()
    assert len(telescopes) == 1

    # Clean up for other methods
    db_session.delete(telescope)
    db_session.commit()
    db_session.close()
