import mock
from banzai.tests.utils import FakeContext

from banzai.realtime import need_to_process_image

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


@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_processed_image')
@mock.patch('banzai.realtime.get_primary_header')
def test_no_processing_if_previous_success(mock_header, mock_processed, mock_md5):
    mock_processed.return_value = FakeRealtimeImage(success=True, checksum=md5_hash1)
    mock_md5.return_value = md5_hash1
    assert not need_to_process_image('test.fits', FakeContext())


@mock.patch('banzai.dbs.commit_processed_image')
@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_processed_image')
@mock.patch('banzai.realtime.get_primary_header')
def test_do_process_if_never_tried(mock_header, mock_processed, mock_md5, mock_commit):
    mock_processed.return_value = FakeRealtimeImage(success=False, checksum=md5_hash1, tries=0)
    mock_md5.return_value = md5_hash1
    assert need_to_process_image('test.fits', FakeContext())


@mock.patch('banzai.dbs.commit_processed_image')
@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_processed_image')
@mock.patch('banzai.realtime.get_primary_header')
def test_do_process_if_tries_less_than_max(mock_header, mock_processed, mock_md5, mock_commit):
    mock_processed.return_value = FakeRealtimeImage(success=False, checksum=md5_hash1, tries=3)
    mock_md5.return_value = md5_hash1
    assert need_to_process_image('test.fits', FakeContext(), max_tries=5)


@mock.patch('banzai.dbs.commit_processed_image')
@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_processed_image')
@mock.patch('banzai.realtime.get_primary_header')
def test_no_processing_if_tries_at_max(mock_header, mock_processed, mock_md5, mock_commit):
    max_tries = 5
    mock_processed.return_value = FakeRealtimeImage(success=False, checksum=md5_hash1, tries=max_tries)
    mock_md5.return_value = md5_hash1
    assert not need_to_process_image('test.fits', FakeContext(), max_tries=max_tries)


@mock.patch('banzai.dbs.commit_processed_image')
@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_processed_image')
@mock.patch('banzai.realtime.get_primary_header')
def test_do_process_if_new_checksum(mock_header, mock_processed, mock_md5, mock_commit):
    # assert that tries and success are reset to 0
    image = FakeRealtimeImage(success=True, checksum=md5_hash1, tries=3)
    mock_processed.return_value = image
    mock_md5.return_value = md5_hash2
    assert need_to_process_image('test.fits', FakeContext())
    assert not image.success
    assert image.tries == 0
    assert image.checksum == md5_hash2
