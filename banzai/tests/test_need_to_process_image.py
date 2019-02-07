import operator

import mock

from banzai.realtime import need_to_process_image
from banzai.context import InstrumentCriterion

md5_hash1 = '49a6bb35cdd3859224c0214310b1d9b6'
md5_hash2 = 'aec5ef355e7e43a59fedc88ac95caed6'


class FakeInstrument(object):
    def __init__(self, schedulable=True):
        self.schedulable = schedulable


class FakeRealtimeImage(object):
    def __init__(self, success=False, checksum=md5_hash1, tries=0):
        self.success = success
        self.checksum = checksum
        self.tries = tries


@mock.patch('banzai.dbs.get_instrument_for_file')
def test_no_processing_if_instrument_is_not_schedulable(mock_instrument):
    mock_instrument.return_value = FakeInstrument(schedulable=False)
    assert not need_to_process_image('test.fits', [InstrumentCriterion('schedulable', operator.eq, True)])


@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_processed_image')
@mock.patch('banzai.dbs.get_instrument_for_file')
def test_no_processing_if_previous_success(mock_instrument, mock_processed, mock_md5):
    mock_instrument.return_value = FakeInstrument(schedulable=True)
    mock_processed.return_value = FakeRealtimeImage(success=True, checksum=md5_hash1)
    mock_md5.return_value = md5_hash1
    assert not need_to_process_image('test.fits', [])


@mock.patch('banzai.dbs.commit_processed_image')
@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_processed_image')
@mock.patch('banzai.dbs.get_instrument_for_file')
def test_do_process_if_never_tried(mock_instrument, mock_processed, mock_md5, mock_commit):
    mock_instrument.return_value = FakeInstrument(schedulable=True)
    mock_processed.return_value = FakeRealtimeImage(success=False, checksum=md5_hash1, tries=0)
    mock_md5.return_value = md5_hash1
    assert need_to_process_image('test.fits', [])


@mock.patch('banzai.dbs.commit_processed_image')
@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_processed_image')
@mock.patch('banzai.dbs.get_instrument_for_file')
def test_do_process_if_tries_less_than_max(mock_instrument, mock_processed, mock_md5, mock_commit):
    mock_instrument.return_value = FakeInstrument(schedulable=True)
    mock_processed.return_value = FakeRealtimeImage(success=False, checksum=md5_hash1, tries=3)
    mock_md5.return_value = md5_hash1
    assert need_to_process_image('test.fits', [], max_tries=5)


@mock.patch('banzai.dbs.commit_processed_image')
@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_processed_image')
@mock.patch('banzai.dbs.get_instrument_for_file')
def test_no_processing_if_tries_at_max(mock_instrument, mock_processed, mock_md5, mock_commit):
    max_tries = 5
    mock_instrument.return_value = FakeInstrument(schedulable=True)
    mock_processed.return_value = FakeRealtimeImage(success=False, checksum=md5_hash1, tries=max_tries)
    mock_md5.return_value = md5_hash1
    assert not need_to_process_image('test.fits', [], max_tries=max_tries)


@mock.patch('banzai.dbs.commit_processed_image')
@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_processed_image')
@mock.patch('banzai.dbs.get_instrument_for_file')
def test_do_process_if_new_checksum(mock_instrument, mock_processed, mock_md5, mock_commit):
    # assert that tries and success are reset to 0
    mock_instrument.return_value = FakeInstrument(schedulable=True)
    image = FakeRealtimeImage(success=True, checksum=md5_hash1, tries=3)
    mock_processed.return_value = image
    mock_md5.return_value = md5_hash2
    assert need_to_process_image('test.fits', [])
    assert not image.success
    assert image.tries == 0
    assert image.checksum == md5_hash2
