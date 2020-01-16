import mock

from banzai.tests.utils import FakeContext
from banzai.utils import realtime_utils

fits_queue_message = {
    'path': '/archive/engineering/lsc/fa15/20200114/processed/lsc1m005-fa15-20200114-0129-d91.fits.fz'}

archived_fits_queue_message = {'SITEID': 'lsc',
                               'INSTRUME': 'fa15',
                               'DAY-OBS': '20200114',
                               'RLEVEL': 91,
                               'filename': 'lsc1m005-fa15-20200114-0129-d91.fits.fz'}


def test_get_filename_from_info_fits_queue():
    filename = realtime_utils.get_filename_from_info(fits_queue_message)

    assert filename == 'lsc1m005-fa15-20200114-0129-d91.fits.fz'


def test_get_filename_from_info_archived_fits_queue():
    filename = realtime_utils.get_filename_from_info(archived_fits_queue_message)

    assert filename == 'lsc1m005-fa15-20200114-0129-d91.fits.fz'


def test_get_local_path_from_info_fits_queue():
    context = FakeContext()

    filepath = realtime_utils.get_local_path_from_info(fits_queue_message, context)

    assert filepath == fits_queue_message['path']


def test_get_local_path_from_info_archived_fits_queue():
    context = FakeContext()

    filepath = realtime_utils.get_local_path_from_info(archived_fits_queue_message, context)

    assert filepath == '/tmp/lsc/fa15/20200114/processed/lsc1m005-fa15-20200114-0129-d91.fits.fz'


@mock.patch('banzai.utils.realtime_utils.os.path.isfile', return_value=True)
def test_need_to_get_from_s3_fits_queue(mock_file_exists):
    context = FakeContext()
    assert not realtime_utils.need_to_get_from_s3(fits_queue_message, context)


@mock.patch('banzai.utils.realtime_utils.os.path.isfile', return_value=False)
def test_need_to_get_from_s3_archived_fits_queue_no_local_file(mock_file_exists):
    context = FakeContext()
    assert realtime_utils.need_to_get_from_s3(archived_fits_queue_message, context)


@mock.patch('banzai.utils.realtime_utils.os.path.isfile', return_value=True)
def test_need_to_get_from_s3_archived_fits_queue_local_file_exists(mock_file_exists):
    context = FakeContext()
    assert not realtime_utils.need_to_get_from_s3(archived_fits_queue_message, context)


def test_is_s3_queue_message_archived_fits():
    assert realtime_utils.is_s3_queue_message(archived_fits_queue_message)


def test_is_s3_queue_message_fits_queue():
    assert not realtime_utils.is_s3_queue_message(fits_queue_message)
