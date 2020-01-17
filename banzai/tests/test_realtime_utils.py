import os
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

master_file_info = {'frameid': 1234,
                    'path': '/archive/engineering/lsc/fa15/20200114/processed/lsc1m005-fa15-20200114-skyflat-center-bin2x2-w.fits.fz'}


def test_get_filename_from_info_fits_queue():
    filename = realtime_utils.get_filename_from_info(fits_queue_message)

    assert filename == 'lsc1m005-fa15-20200114-0129-d91.fits.fz'


def test_get_filename_from_info_archived_fits_queue():
    filename = realtime_utils.get_filename_from_info(archived_fits_queue_message)

    assert filename == 'lsc1m005-fa15-20200114-0129-d91.fits.fz'


def test_get_local_path_from_info_fits_queue():
    context = FakeContext()

    filepath = realtime_utils.get_local_path_from_info(fits_queue_message, context)
    base_filename, file_extension = os.path.splitext(os.path.basename(filepath))

    assert filepath == fits_queue_message['path']
    assert base_filename == 'lsc1m005-fa15-20200114-0129-d91.fits'
    assert file_extension == '.fz'


def test_get_local_path_from_info_archived_fits_queue():
    context = FakeContext()

    filepath = realtime_utils.get_local_path_from_info(archived_fits_queue_message, context)
    base_filename, file_extension = os.path.splitext(os.path.basename(filepath))

    assert filepath == '/tmp/lsc/fa15/20200114/processed/lsc1m005-fa15-20200114-0129-d91.fits.fz'
    assert base_filename == 'lsc1m005-fa15-20200114-0129-d91.fits'
    assert file_extension == '.fz'


def test_get_local_path_from_master_cal():
    context = FakeContext()

    filepath = realtime_utils.get_local_path_from_info(master_file_info, context)
    base_filename, file_extension = os.path.splitext(os.path.basename(filepath))

    assert filepath == '/archive/engineering/lsc/fa15/20200114/processed/lsc1m005-fa15-20200114-skyflat-center-bin2x2-w.fits.fz'
    assert base_filename == 'lsc1m005-fa15-20200114-skyflat-center-bin2x2-w.fits'
    assert file_extension == '.fz'


def test_is_s3_queue_message_archived_fits():
    assert realtime_utils.is_s3_queue_message(archived_fits_queue_message)


def test_is_s3_queue_message_fits_queue():
    assert not realtime_utils.is_s3_queue_message(fits_queue_message)

