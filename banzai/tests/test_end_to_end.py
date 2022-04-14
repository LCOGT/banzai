import os
from glob import glob
import argparse

import pytest
import mock
import time
from datetime import datetime

from dateutil.parser import parse

from banzai import settings
from types import ModuleType
from banzai.celery import app, schedule_calibration_stacking
from banzai.dbs import get_session, CalibrationImage, get_timezone, populate_instrument_tables
from banzai.dbs import mark_frame
from banzai.utils import fits_utils, file_utils
from banzai.main import add_super_calibration
from banzai.tests.utils import FakeResponse, get_min_and_max_dates, FakeContext
from astropy.io import fits
import pkg_resources

import logging

# TODO: Mock out AWS calls here
# TODO: Mock out archived fits queue structure as well

logger = logging.getLogger('banzai')

app.conf.update(CELERY_TASK_ALWAYS_EAGER=True)

DATA_ROOT = os.path.join(os.sep, 'archive', 'engineering')
SITES = [os.path.basename(site_path) for site_path in glob(os.path.join(DATA_ROOT, '???'))]
INSTRUMENTS = [os.path.join(site, os.path.basename(instrument_path)) for site in SITES
               for instrument_path in glob(os.path.join(os.path.join(DATA_ROOT, site, '*')))]

DAYS_OBS = [os.path.join(instrument, os.path.basename(dayobs_path)) for instrument in INSTRUMENTS
            for dayobs_path in glob(os.path.join(DATA_ROOT, instrument, '20*'))]

TEST_PACKAGE = 'banzai.tests'
CONFIGDB_FILENAME = pkg_resources.resource_filename(TEST_PACKAGE, 'data/configdb_example.json')


def celery_join():
    celery_inspector = app.control.inspect()
    log_counter = 0
    while True:
        queues = [celery_inspector.active(), celery_inspector.scheduled(), celery_inspector.reserved()]
        time.sleep(1)
        log_counter += 1
        if log_counter % 30 == 0:
            logger.info('Processing: ' + '. ' * (log_counter // 30))
        queue_names = []
        for queue in queues:
            if queue is not None:
                queue_names += queue.keys()
        if 'celery@banzai-celery-worker' not in queue_names:
            logger.warning('No valid celery queues were detected, retrying...', extra_tags={'queues': queues})
            # Reset the celery connection
            celery_inspector = app.control.inspect()
            continue
        if all(queue is None or len(queue['celery@banzai-celery-worker']) == 0 for queue in queues):
            break


def run_reduce_individual_frames(raw_filenames):
    logger.info('Reducing individual frames for filenames: {filenames}'.format(filenames=raw_filenames))
    for day_obs in DAYS_OBS:
        raw_path = os.path.join(DATA_ROOT, day_obs, 'raw')
        for filename in glob(os.path.join(raw_path, raw_filenames)):
            file_utils.post_to_archive_queue(filename, os.getenv('FITS_BROKER'), exchange_name=os.getenv('FITS_EXCHANGE'))
    celery_join()
    logger.info('Finished reducing individual frames for filenames: {filenames}'.format(filenames=raw_filenames))


def stack_calibrations(frame_type):
    logger.info('Stacking calibrations for frame type: {frame_type}'.format(frame_type=frame_type))
    for day_obs in DAYS_OBS:
        site, camera, dayobs = day_obs.split('/')
        timezone = get_timezone(site, db_address=os.environ['DB_ADDRESS'])
        min_date, max_date = get_min_and_max_dates(timezone, dayobs=dayobs)
        runtime_context = dict(processed_path=DATA_ROOT, log_level='debug', post_to_archive=False,
                               post_to_opensearch=False, fpack=True, reduction_level=91,
                               db_address=os.environ['DB_ADDRESS'], opensearch_qc_index='banzai_qc',
                               opensearch_url='https://opensearch.lco.global/',
                               no_bpm=False, ignore_schedulability=True, use_only_older_calibrations=False,
                               preview_mode=False, max_tries=5, broker_url=os.getenv('FITS_BROKER'),
                               no_file_cache=False)
        for setting in dir(settings):
            if '__' != setting[:2] and not isinstance(getattr(settings, setting), ModuleType):
                runtime_context[setting] = getattr(settings, setting)
        schedule_calibration_stacking(site, runtime_context, min_date=min_date, max_date=max_date,
                                      frame_types=[frame_type])
    celery_join()
    logger.info('Finished stacking calibrations for frame type: {frame_type}'.format(frame_type=frame_type))


def mark_frames_as_good(raw_filenames):
    logger.info('Marking frames as good for filenames: {filenames}'.format(filenames=raw_filenames))
    for day_obs in DAYS_OBS:
        for filename in glob(os.path.join(DATA_ROOT, day_obs, 'processed', raw_filenames)):
            mark_frame(os.path.basename(filename), "good", db_address=os.environ['DB_ADDRESS'])
    logger.info('Finished marking frames as good for filenames: {filenames}'.format(filenames=raw_filenames))


def get_expected_number_of_calibrations(raw_filenames, calibration_type):
    context = FakeContext()
    context.db_address = os.environ['DB_ADDRESS']
    number_of_stacks_that_should_have_been_created = 0
    for day_obs in DAYS_OBS:
        raw_filenames_for_this_dayobs = glob(os.path.join(DATA_ROOT, day_obs, 'raw', raw_filenames))
        if calibration_type.lower() == 'skyflat':
            # Group by filter
            observed_filters = []
            for raw_filename in raw_filenames_for_this_dayobs:
                skyflat_hdu, skyflat_filename, frame_id = fits_utils.open_fits_file({'path': raw_filename}, context)
                observed_filters.append(skyflat_hdu[0].header.get('FILTER'))
            observed_filters = set(observed_filters)
            number_of_stacks_that_should_have_been_created += len(observed_filters)
        else:
            # Just one calibration per night
            if len(raw_filenames_for_this_dayobs) > 0:
                number_of_stacks_that_should_have_been_created += 1
    return number_of_stacks_that_should_have_been_created


def run_check_if_stacked_calibrations_were_created(raw_filenames, calibration_type):
    created_stacked_calibrations = []
    number_of_stacks_that_should_have_been_created = get_expected_number_of_calibrations(raw_filenames, calibration_type)
    for day_obs in DAYS_OBS:
        created_stacked_calibrations += glob(os.path.join(DATA_ROOT, day_obs, 'processed',
                                                          '*' + calibration_type.lower() + '*.fits*'))
    assert number_of_stacks_that_should_have_been_created > 0
    assert len(created_stacked_calibrations) == number_of_stacks_that_should_have_been_created


def run_check_if_stacked_calibrations_are_in_db(raw_filenames, calibration_type):
    number_of_stacks_that_should_have_been_created = get_expected_number_of_calibrations(raw_filenames, calibration_type)
    with get_session(os.environ['DB_ADDRESS']) as db_session:
        calibrations_in_db = db_session.query(CalibrationImage).filter(CalibrationImage.type == calibration_type)
        calibrations_in_db = calibrations_in_db.filter(CalibrationImage.is_master).all()
    assert number_of_stacks_that_should_have_been_created > 0
    assert len(calibrations_in_db) == number_of_stacks_that_should_have_been_created


def observation_portal_side_effect(*args, **kwargs):
    site = kwargs['params']['site']
    start = datetime.strftime(parse(kwargs['params']['start_after']).replace(tzinfo=None).date(), '%Y%m%d')
    filename = 'test_obs_portal_response_{site}_{start}.json'.format(site=site, start=start)
    filename = pkg_resources.resource_filename(TEST_PACKAGE, 'data/{filename}'.format(filename=filename))
    if not os.path.exists(filename):
        filename = pkg_resources.resource_filename(TEST_PACKAGE, 'data/test_obs_portal_null.json')
    return FakeResponse(filename)


# TODO: Add photometric catalog mock response
# Note this is complicated by the fact that things are running as celery tasks.
@pytest.mark.e2e
@pytest.fixture(scope='module')
@mock.patch('banzai.main.argparse.ArgumentParser.parse_args')
@mock.patch('banzai.main.file_utils.post_to_ingester', return_value={'frameid': None})
@mock.patch('banzai.dbs.requests.get', return_value=FakeResponse(CONFIGDB_FILENAME))
def init(configdb, mock_ingester, mock_args):
    os.system(f'banzai_create_db --db-address={os.environ["DB_ADDRESS"]}')
    populate_instrument_tables(db_address=os.environ["DB_ADDRESS"], configdb_address='http://fakeconfigdb')
    for instrument in INSTRUMENTS:
        for bpm_filepath in glob(os.path.join(DATA_ROOT, instrument, 'bpm/*bpm*')):
            mock_args.return_value = argparse.Namespace(filepath=bpm_filepath, db_address=os.environ['DB_ADDRESS'], log_level='debug')
            add_super_calibration()


@pytest.mark.e2e
@pytest.mark.master_bias
class TestMasterBiasCreation:
    @pytest.fixture(autouse=True)
    @mock.patch('banzai.utils.observation_utils.requests.get', side_effect=observation_portal_side_effect)
    def stack_bias_frames(self, mock_observation_portal, init):
        run_reduce_individual_frames('*b00.fits*')
        mark_frames_as_good('*b91.fits*')
        stack_calibrations('bias')

    def test_if_stacked_bias_frame_was_created(self):
        run_check_if_stacked_calibrations_were_created('*b00.fits*', 'bias')
        run_check_if_stacked_calibrations_are_in_db('*b00.fits*', 'BIAS')


@pytest.mark.e2e
@pytest.mark.master_dark
class TestMasterDarkCreation:
    @pytest.fixture(autouse=True)
    @mock.patch('banzai.utils.observation_utils.requests.get', side_effect=observation_portal_side_effect)
    def stack_dark_frames(self, mock_observation_portal):
        run_reduce_individual_frames('*d00.fits*')
        mark_frames_as_good('*d91.fits*')
        stack_calibrations('dark')

    def test_if_stacked_dark_frame_was_created(self):
        run_check_if_stacked_calibrations_were_created('*d00.fits*', 'dark')
        run_check_if_stacked_calibrations_are_in_db('*d00.fits*', 'DARK')


@pytest.mark.e2e
@pytest.mark.master_flat
class TestMasterFlatCreation:
    @pytest.fixture(autouse=True)
    @mock.patch('banzai.utils.observation_utils.requests.get', side_effect=observation_portal_side_effect)
    def stack_flat_frames(self, mock_observation_portal):
        run_reduce_individual_frames('*f00.fits*')
        mark_frames_as_good('*f91.fits*')
        stack_calibrations('skyflat')

    def test_if_stacked_flat_frame_was_created(self):
        run_check_if_stacked_calibrations_were_created('*f00.fits*', 'skyflat')
        run_check_if_stacked_calibrations_are_in_db('*f00.fits*', 'SKYFLAT')


@pytest.mark.e2e
@pytest.mark.science_files
class TestScienceFileCreation:
    @pytest.fixture(autouse=True)
    @mock.patch('banzai.utils.observation_utils.requests.get', side_effect=observation_portal_side_effect)
    def reduce_science_frames(self, mock_observation_portal):
        run_reduce_individual_frames('*e00.fits*')

    def test_if_science_frames_were_created(self):
        expected_files = []
        created_files = []
        for day_obs in DAYS_OBS:
            expected_files += [os.path.basename(filename).replace('e00', 'e91')
                               for filename in glob(os.path.join(DATA_ROOT, day_obs, 'raw', '*e00*'))]
            created_files += [os.path.basename(filename) for filename in glob(os.path.join(DATA_ROOT, day_obs,
                                                                                           'processed', '*e91*'))]
        assert len(expected_files) > 0
        for expected_file in expected_files:
            assert expected_file in created_files

    def test_that_photometric_calibration_succeeded(self):
        science_files = []
        for day_obs in DAYS_OBS:
            science_files += [filepath for filepath in glob(os.path.join(DATA_ROOT, day_obs,
                                                                         'processed', '*e91*'))]
        zeropoints = [fits.open(file)['SCI'].header.get('L1ZP') for file in science_files]
        # check that at least one of our images contains a zeropoint
        assert zeropoints.count(None) != len(zeropoints)
