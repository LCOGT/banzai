import mock
import pytest

from banzai.tests.utils import FakeContext
from banzai.utils.realtime_utils import need_to_process_image
import datetime

md5_hash1 = '49a6bb35cdd3859224c0214310b1d9b6'
md5_hash2 = 'aec5ef355e7e43a59fedc88ac95caed6'

pytestmark = pytest.mark.need_to_process_image


class FakeRealtimeImage(object):
    def __init__(self, success=False, checksum=md5_hash1, tries=0, block_end_date=None):
        self.success = success
        self.checksum = checksum
        self.tries = tries


@pytest.mark.parametrize('reduction_level, success, expected', [
    pytest.param(0, False, True, id='raw'),
    pytest.param(45, False, True, id='configured-nonzero'),
    pytest.param(45, True, False, id='configured-successful-duplicate'),
    pytest.param(91, False, False, id='unsupported-nonzero'),
])
@mock.patch('banzai.utils.realtime_utils.logger')
@mock.patch('banzai.utils.image_utils.image_can_be_processed', return_value=True)
@mock.patch('banzai.utils.realtime_utils.import_utils.import_attribute')
@mock.patch('banzai.dbs.commit_processed_image')
@mock.patch('banzai.dbs.get_processed_image')
def test_archive_reduction_level_routing(mock_processed, mock_commit, mock_import, mock_can_process, mock_logger,
                                         reduction_level, success, expected):
    image = FakeRealtimeImage(success=success, checksum=md5_hash1)
    mock_processed.return_value = image
    test_image = mock.MagicMock(meta={'RLEVEL': reduction_level}, obstype='EXPOSE')
    factory = mock.MagicMock()
    factory.observation_frame_class.return_value = test_image
    factory.get_instrument_from_header.return_value = mock.sentinel.instrument
    mock_import.return_value.return_value = factory
    context = FakeContext(
        START_STAGE_BY_REDUCTION_LEVEL={'45': 'banzai.photometry.SourceDetector'},
        delay_to_block_end=False,
    )
    file_info = {
        'frameid': 42,
        'filename': 'test.fits',
        'version_set': [{'md5': md5_hash1}],
        'RLEVEL': reduction_level,
        'OBSTYPE': 'EXPOSE',
    }

    assert need_to_process_image(file_info, context, mock.MagicMock()) is expected
    if reduction_level == 91:
        mock_logger.error.assert_called_once_with(
            'Image has unsupported reduction level. Aborting.',
            extra_tags={'filename': 'test.fits', 'reduction_level': '91'},
        )


@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_processed_image')
@mock.patch('banzai.utils.fits_utils.get_primary_header')
@mock.patch('banzai.utils.image_utils.image_can_be_processed')
def test_no_processing_if_previous_success(mock_can_process, mock_header, mock_processed, mock_md5):
    mock_task = mock.MagicMock()
    mock_can_process.return_value = True
    mock_processed.return_value = FakeRealtimeImage(success=True, checksum=md5_hash1)
    mock_md5.return_value = md5_hash1
    assert not need_to_process_image({'path': 'test.fits'}, FakeContext(), mock_task)


@mock.patch('banzai.dbs.commit_processed_image')
@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_processed_image')
@mock.patch('banzai.utils.fits_utils.get_primary_header')
@mock.patch('banzai.utils.image_utils.image_can_be_processed')
def test_do_process_if_never_tried(mock_can_process, mock_header, mock_processed, mock_md5, mock_commit):
    mock_task = mock.MagicMock()
    mock_can_process.return_value = True
    mock_processed.return_value = FakeRealtimeImage(success=False, checksum=md5_hash1, tries=0)
    mock_md5.return_value = md5_hash1
    assert need_to_process_image({'path': 'test.fits'}, FakeContext(), mock_task)


@mock.patch('banzai.dbs.commit_processed_image')
@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_processed_image')
@mock.patch('banzai.utils.fits_utils.get_primary_header')
@mock.patch('banzai.utils.image_utils.image_can_be_processed')
def test_do_process_if_tries_less_than_max(mock_can_process, mock_header, mock_processed, mock_md5, mock_commit):
    mock_task = mock.MagicMock()
    mock_can_process.return_value = True
    mock_processed.return_value = FakeRealtimeImage(success=False, checksum=md5_hash1, tries=3)
    mock_md5.return_value = md5_hash1
    context = FakeContext()
    context.max_tries = 5
    assert need_to_process_image({'path': 'test.fits'}, context, mock_task)


@mock.patch('banzai.dbs.commit_processed_image')
@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_processed_image')
@mock.patch('banzai.utils.fits_utils.get_primary_header')
@mock.patch('banzai.utils.image_utils.image_can_be_processed')
def test_no_processing_if_tries_at_max(mock_can_process, mock_header, mock_processed, mock_md5, mock_commit):
    mock_task = mock.MagicMock()
    mock_can_process.return_value = True
    max_tries = 5
    mock_processed.return_value = FakeRealtimeImage(success=False, checksum=md5_hash1, tries=max_tries)
    mock_md5.return_value = md5_hash1
    context = FakeContext()
    context.max_tries = max_tries
    assert not need_to_process_image({'path': 'test.fits'}, context, mock_task)


@mock.patch('banzai.dbs.commit_processed_image')
@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_processed_image')
@mock.patch('banzai.utils.fits_utils.get_primary_header')
@mock.patch('banzai.utils.image_utils.image_can_be_processed')
def test_do_process_if_new_checksum(mock_can_process, mock_header, mock_processed, mock_md5, mock_commit):
    # assert that tries and success are reset to 0
    mock_task = mock.MagicMock()
    image = FakeRealtimeImage(success=True, checksum=md5_hash1, tries=3)
    mock_can_process.return_value = True
    mock_processed.return_value = image
    mock_md5.return_value = md5_hash2
    assert need_to_process_image({'path': 'test.fits'}, FakeContext(), mock_task)
    assert not image.success
    assert image.tries == 0
    assert image.checksum == md5_hash2
