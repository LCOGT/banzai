import os
from glob import glob

import pytest
import mock
import time
from datetime import datetime

from dateutil.parser import parse

from banzai import settings
from types import ModuleType
from banzai.celery import app, schedule_calibration_stacking
from banzai.dbs import get_session, CalibrationImage, get_timezone, populate_instrument_tables
from banzai.dbs import mark_frame, query_for_instrument
from banzai.utils import file_utils
from banzai.tests.utils import FakeResponse, get_min_and_max_dates, FakeContext
from astropy.io import fits, ascii
import pkg_resources
from banzai.logs import get_logger

# TODO: Mock out AWS calls here
# TODO: Mock out archived fits queue structure as well

logger = get_logger()

app.conf.update(CELERY_TASK_ALWAYS_EAGER=True)

TEST_PACKAGE = 'banzai.tests'
TEST_FRAMES = ascii.read(pkg_resources.resource_filename(TEST_PACKAGE, 'data/test_data.dat'))

PRECAL_FRAMES = ascii.read(pkg_resources.resource_filename(TEST_PACKAGE, 'data/test_precals.dat'))

DATA_ROOT = os.path.join(os.sep, 'archive', 'engineering')
# Use the LCO filenaming convention to infer the sites
SITES = set([frame[:3] for frame in TEST_FRAMES['filename']])
INSTRUMENTS = set([os.path.join(frame[:3], frame.split('-')[1]) for frame in TEST_FRAMES['filename']])

DAYS_OBS = set([os.path.join(frame[:3], frame.split('-')[1], frame.split('-')[2]) for frame in TEST_FRAMES['filename']])

CONFIGDB_FILENAME = pkg_resources.resource_filename(TEST_PACKAGE, 'data/configdb_example.json')


def celery_join():
    celery_inspector = app.control.inspect()
    celery_connection = app.connection()
    celery_channel = celery_connection.channel()
    log_counter = 0
    while True:
        time.sleep(1)
        queues = [celery_inspector.active(), celery_inspector.scheduled(), celery_inspector.reserved()]
        log_counter += 1
        if log_counter % 30 == 0:
            logger.info('Processing: ' + '. ' * (log_counter // 30))
        queue_names = []
        for queue in queues:
            if queue is not None:
                queue_names += queue.keys()
        if 'celery@celery-worker' not in queue_names or 'celery@large-celery-worker' not in queue_names:
            logger.warning('Valid celery queues were not detected, retrying...', extra_tags={'queues': queues})
            # Reset the celery connection
            celery_inspector = app.control.inspect()
            continue
        jobs_left = celery_channel.queue_declare('e2e_large_task_queue').message_count
        jobs_left += celery_channel.queue_declare('e2e_task_queue').message_count
        no_active_jobs = all(queue is None or
                             (len(queue['celery@celery-worker']) == 0 and
                              len(queue['celery@large-celery-worker']) == 0)
                             for queue in queues)
        if no_active_jobs and jobs_left == 0:
            break


def run_reduce_individual_frames(filename_pattern):
    logger.info('Reducing individual frames for filenames: {filenames}'.format(filenames=filename_pattern))
    for frame in TEST_FRAMES:
        if filename_pattern in frame['filename']:
            file_utils.post_to_archive_queue(frame['filename'], frame['frameid'],
                                             os.getenv('FITS_BROKER'),
                                             exchange_name=os.getenv('FITS_EXCHANGE'),
                                             SITEID=frame['site'], INSTRUME=frame['instrument'])
    celery_join()
    logger.info('Finished reducing individual frames for filenames: {filenames}'.format(filenames=filename_pattern))


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


def get_expected_number_of_calibrations(raw_filename_pattern, calibration_type):
    context = FakeContext()
    context.db_address = os.environ['DB_ADDRESS']
    number_of_stacks_that_should_have_been_created = 0
    for day_obs in DAYS_OBS:
        site, instrument, dayobs = day_obs.split('/')
        raw_frames_for_this_dayobs = [
            frame for frame in TEST_FRAMES
            if site in frame['filename'] and instrument in frame['filename']
            and dayobs in frame['filename'] and raw_filename_pattern in frame['filename']
        ]
        if calibration_type.lower() == 'skyflat':
            # Group by filter
            observed_filters = []
            for frame in raw_frames_for_this_dayobs:
                observed_filters.append(frame['filter'])
            observed_filters = set(observed_filters)
            number_of_stacks_that_should_have_been_created += len(observed_filters)
        else:
            # Just one calibration per night
            if len(raw_frames_for_this_dayobs) > 0:
                number_of_stacks_that_should_have_been_created += 1
    return number_of_stacks_that_should_have_been_created


def run_check_if_stacked_calibrations_were_created(raw_file_pattern, calibration_type):
    created_stacked_calibrations = []
    number_of_stacks_that_should_have_been_created = get_expected_number_of_calibrations(raw_file_pattern, calibration_type)
    for day_obs in DAYS_OBS:
        created_stacked_calibrations += glob(os.path.join(DATA_ROOT, day_obs, 'processed',
                                                          '*' + calibration_type.lower() + '*.fits*'))
    assert number_of_stacks_that_should_have_been_created > 0
    assert len(created_stacked_calibrations) == number_of_stacks_that_should_have_been_created


def run_check_if_stacked_calibrations_are_in_db(raw_file_pattern, calibration_type):
    number_of_stacks_that_should_have_been_created = get_expected_number_of_calibrations(raw_file_pattern,
                                                                                         calibration_type)
    with get_session(os.environ['DB_ADDRESS']) as db_session:
        calibrations_in_db = db_session.query(CalibrationImage).filter(CalibrationImage.type == calibration_type)
        calibrations_in_db = calibrations_in_db.filter(CalibrationImage.is_master).all()
    assert number_of_stacks_that_should_have_been_created > 0
    assert len(calibrations_in_db) == number_of_stacks_that_should_have_been_created


def observation_portal_side_effect(*args, **kwargs):
    # To produce the mock observation portal response, we need to modify the response from
    # this type of url
    # https://observe.lco.global/api/observations/?enclosure=aqwa&telescope=0m4a&priority=&state=COMPLETED&time_span=&start_after=2024-07-23&start_before=&end_after=&end_before=2024-07-25&modified_after=&created_after=&created_before=&request_id=&request_group_id=&user=&proposal=calibrate&instrument_type=0M4-SCICAM-QHY600&configuration_type=SKY_FLAT&ordering=
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
@mock.patch('banzai.dbs.requests.get', return_value=FakeResponse(CONFIGDB_FILENAME))
def init(configdb):
    os.system(f'banzai_create_db --db-address={os.environ["DB_ADDRESS"]}')
    populate_instrument_tables(db_address=os.environ["DB_ADDRESS"], configdb_address='http://fakeconfigdb')

    for frame in PRECAL_FRAMES:
        instrument = query_for_instrument(camera=frame['instrument'],
                                          site=frame['site'],
                                          db_address=os.environ['DB_ADDRESS'])
        calimage = CalibrationImage(
            type=frame['obstype'],
            filename=frame['filename'],
            frameid=f'{frame["frameid"]:d}',
            dateobs=datetime.strptime(frame['dateobs'], '%Y-%m-%d'),
            datecreated=datetime(2023, 11, 19),
            instrument_id=instrument.id,
            is_master=True, is_bad=False,
            attributes={'binning': frame['binning'], 'configuration_mode': frame['mode']}
        )
        with get_session(os.environ['DB_ADDRESS']) as db_session:
            db_session.add(calimage)
            db_session.commit()


@pytest.mark.e2e
@pytest.mark.master_bias
class TestMasterBiasCreation:
    @pytest.fixture(autouse=True)
    @mock.patch('banzai.utils.observation_utils.requests.get', side_effect=observation_portal_side_effect)
    def stack_bias_frames(self, mock_observation_portal, init):
        run_reduce_individual_frames('b00.fits')
        mark_frames_as_good('*b91.fits*')
        stack_calibrations('bias')

    def test_if_stacked_bias_frame_was_created(self):
        run_check_if_stacked_calibrations_were_created('b00.fits', 'bias')
        run_check_if_stacked_calibrations_are_in_db('b00.fits', 'BIAS')


@pytest.mark.e2e
@pytest.mark.master_dark
class TestMasterDarkCreation:
    @pytest.fixture(autouse=True)
    @mock.patch('banzai.utils.observation_utils.requests.get', side_effect=observation_portal_side_effect)
    def stack_dark_frames(self, mock_observation_portal):
        run_reduce_individual_frames('d00.fits')
        mark_frames_as_good('*d91.fits*')
        stack_calibrations('dark')

    def test_if_stacked_dark_frame_was_created(self):
        run_check_if_stacked_calibrations_were_created('d00.fits', 'dark')
        run_check_if_stacked_calibrations_are_in_db('d00.fits', 'DARK')


@pytest.mark.e2e
@pytest.mark.master_flat
class TestMasterFlatCreation:
    @pytest.fixture(autouse=True)
    @mock.patch('banzai.utils.observation_utils.requests.get', side_effect=observation_portal_side_effect)
    def stack_flat_frames(self, mock_observation_portal):
        run_reduce_individual_frames('f00.fits')
        mark_frames_as_good('*f91.fits*')
        stack_calibrations('skyflat')

    def test_if_stacked_flat_frame_was_created(self):
        run_check_if_stacked_calibrations_were_created('f00.fits', 'skyflat')
        run_check_if_stacked_calibrations_are_in_db('f00.fits', 'SKYFLAT')


@pytest.mark.e2e
@pytest.mark.science_files
class TestScienceFileCreation:
    @pytest.fixture(autouse=True, scope='class')
    @mock.patch('banzai.utils.observation_utils.requests.get', side_effect=observation_portal_side_effect)
    def reduce_science_frames(self, mock_observation_portal):
        run_reduce_individual_frames('e00.fits')
        run_reduce_individual_frames('x00.fits')

    def test_if_science_frames_were_created(self):
        expected_files = []
        created_files = []
        for day_obs in DAYS_OBS:
            for extension in ['e00', 'x00']:
                expected_files += [filename.replace(extension, extension.replace('00', '91'))
                                   for filename in TEST_FRAMES['filename'] if f'{extension}.fits' in filename]
                created_files += [os.path.basename(filename)
                                  for filename in glob(os.path.join(DATA_ROOT, day_obs, 'processed',
                                                                    f'*{extension.replace("00", "91")}*'))]
        assert len(expected_files) > 0
        for expected_file in expected_files:
            assert expected_file in created_files

    def test_that_photometric_calibration_succeeded(self):
        science_files = []
        for day_obs in DAYS_OBS:
            for extension in ['e91', 'x91']:
                science_files += [filepath for filepath in
                                  glob(os.path.join(DATA_ROOT, day_obs, 'processed', f'*{extension}*'))]
        zeropoints = [fits.open(filename)['SCI'].header.get('L1ZP') for filename in science_files]
        # check that at least one of our images contains a zeropoint
        assert zeropoints.count(None) != len(zeropoints)
