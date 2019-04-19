import os

import mock

from banzai import dbs
from banzai.tests.utils import FakeResponse


@mock.patch('banzai.dbs.requests.get', return_value=FakeResponse())
def setup_module(mockrequests):
    dbs.create_db('.', db_address='sqlite:///test.db')


def teardown_module():
    os.remove('test.db')


def test_add_or_update():
    db_session = dbs.get_session(db_address='sqlite:///test.db')
    # Add a fake telescope
    dbs.add_or_update_record(db_session, dbs.Instrument, {'site': 'bpl', 'camera': 'kb101', 'enclosure': 'doma',
                                                          'telescope': '1m0a', 'name': 'kb101'},
                             {'site': 'bpl', 'camera': 'kb101', 'enclosure': 'doma', 'telescope': '1m0a',
                              'type': 'SBig', 'schedulable': False, 'name': 'kb101'})
    db_session.commit()

    # Make sure it got added
    query = db_session.query(dbs.Instrument).filter(dbs.Instrument.site == 'bpl')
    telescope = query.filter(dbs.Instrument.camera == 'kb101').first()
    assert telescope is not None

    # Update the fake telescope
    dbs.add_or_update_record(db_session, dbs.Instrument, {'site': 'bpl', 'camera': 'kb101', 'enclosure': 'doma',
                                                          'telescope': '1m0a', 'name': 'kb101'},
                             {'site': 'bpl', 'camera': 'kb101', 'enclosure': 'doma', 'telescope': '1m0a',
                              'type': 'SBig', 'schedulable': True})

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


def test_removing_duplicates():
    nres_inst = {'site': 'tlv', 'name': 'nres01', 'camera': 'fa18', 'schedulable': True}
    other_inst = {'site': 'tlv', 'name': 'cam01', 'camera': 'fa12', 'schedulable': True}
    instruments = [other_inst] + [nres_inst]*3
    culled_list = dbs.remove_nres_duplicates(instruments)
    assert len(culled_list) == 2
    assert culled_list[1]['name'] == 'nres01'
    assert culled_list[0]['name'] == 'cam01'


def test_removing_duplicates_favors_scheduluable():
    nres_inst = {'site': 'tlv', 'name': 'nres01', 'camera': 'fa18', 'schedulable': True}
    other_inst = {'site': 'tlv', 'name': 'cam01', 'camera': 'fa12', 'schedulable': True}
    instruments = [other_inst, nres_inst, {'site': 'tlv', 'name': 'nres01', 'camera': 'fa18', 'schedulable': False}]
    culled_list = dbs.remove_nres_duplicates(instruments)
    assert len(culled_list) == 2
    assert culled_list[1]['name'] == 'nres01'
    assert culled_list[0]['name'] == 'cam01'
    assert culled_list[1]['schedulable']


def test_not_removing_singlets():
    nres_inst = {'site': 'tlv', 'name': 'nres01', 'camera': 'fa18', 'schedulable': True}
    other_inst = {'site': 'tlv', 'name': 'cam01', 'camera': 'fa12', 'schedulable': True}
    instruments = [other_inst] + [nres_inst]
    culled_list = dbs.remove_nres_duplicates(instruments)
    assert len(culled_list) == 2
    assert culled_list[1]['name'] == 'nres01'
    assert culled_list[0]['name'] == 'cam01'


def test_fl_cameras_are_added():
    nres_inst = {'site': 'tlv', 'name': 'nres01', 'camera': 'fa18', 'schedulable': True}
    nres_fl_inst = {'site': 'tlv', 'name': 'nres01', 'camera': 'fl18', 'schedulable': True}
    instruments = [nres_inst, nres_fl_inst, nres_fl_inst]
    culled_list = dbs.remove_nres_duplicates(instruments)
    assert len(culled_list) == 2
    assert culled_list[1]['camera'] == 'fl18'
    assert culled_list[0]['camera'] == 'fa18'
