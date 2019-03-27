import os
from glob import glob
import argparse

import pytest
import mock

from banzai import settings
from banzai.main import redis_broker
from banzai.dbs import populate_calibration_table_with_bpms, create_db, get_session, CalibrationImage, get_timezone
from banzai.utils import fits_utils, date_utils, file_utils
from banzai.tests.utils import FakeResponse, get_min_and_max_dates

import logging

logger = logging.getLogger(__name__)

DATA_ROOT = os.path.join(os.sep, 'archive', 'engineering')

SITES = [os.path.basename(site_path) for site_path in glob(os.path.join(DATA_ROOT, '???'))]
INSTRUMENTS = [os.path.join(site, os.path.basename(instrument_path)) for site in SITES
               for instrument_path in glob(os.path.join(os.path.join(DATA_ROOT, site, '*')))]

DAYS_OBS = [os.path.join(instrument, os.path.basename(dayobs_path)) for instrument in INSTRUMENTS
            for dayobs_path in glob(os.path.join(DATA_ROOT, instrument, '201*'))]

ENCLOSURE_DICT = {
    'fl11': 'domb',
    'kb27': 'clma',
    'fs02': 'clma',
}

TELESCOPE_DICT = {
    'fl11': '1m0a',
    'kb27': '0m4b',
    'fs02': '2m0a',
}


def run_end_to_end_tests():
    parser = argparse.ArgumentParser()
    parser.add_argument('--marker', dest='marker', help='PyTest marker to run')
    parser.add_argument('--junit-file', dest='junit_file', help='Path to junit xml file with results')
    parser.add_argument('--code-path', dest='code_path', help='Path to directory with setup.py')
    args = parser.parse_args()
    os.chdir(args.code_path)
    command = 'python setup.py test -a "--durations=0 --junitxml={junit_file} -m {marker}"'

    # Bitshift by 8 because Python encodes exit status in the leftmost 8 bits
    return os.system(command.format(junit_file=args.junit_file, marker=args.marker)) >> 8


def run_reduce_individual_frames(raw_filenames):
    logger.info('Reducing individual frames for filenames: {filenames}'.format(filenames=raw_filenames))
    for day_obs in DAYS_OBS:
        raw_path = os.path.join(DATA_ROOT, day_obs, 'raw')
        for filename in glob(os.path.join(raw_path, raw_filenames)):
            file_utils.post_to_archive_queue(filename, os.getenv('FITS_BROKER_URL'))
    redis_broker.join(settings.REDIS_QUEUE_NAMES['PROCESS_IMAGE'])
    logger.info('Finished reducing individual frames for filenames: {filenames}'.format(filenames=raw_filenames))


def run_stack_calibrations(frame_type):
    logger.info('Stacking calibrations for frame type: {frame_type}'.format(frame_type=frame_type))
    for day_obs in DAYS_OBS:
        raw_path = os.path.join(DATA_ROOT, day_obs, 'raw')
        site, camera, dayobs = day_obs.split('/')
        timezone = get_timezone(site, db_address=os.environ['DB_ADDRESS'])
        min_date, max_date = get_min_and_max_dates(timezone, dayobs, return_string=True)
        command = 'banzai_stack_calibrations --raw-path {raw_path} --frame-type {frame_type} ' \
                  '--site {site} --camera {camera} ' \
                  '--enclosure {enclosure} --telescope {telescope} ' \
                  '--min-date {min_date} --max-date {max_date} ' \
                  '--db-address={db_address} --ignore-schedulability --fpack'
        command = command.format(raw_path=raw_path, frame_type=frame_type, site=site, camera=camera,
                                 enclosure=ENCLOSURE_DICT[camera], telescope=TELESCOPE_DICT[camera],
                                 min_date=min_date, max_date=max_date, db_address=os.environ['DB_ADDRESS'])
        logger.info('Running the following stacking command: {command}'.format(command=command))
        os.system(command)
    redis_broker.join(settings.REDIS_QUEUE_NAMES['SCHEDULE_STACK'])
    logger.info('Finished stacking calibrations for frame type: {frame_type}'.format(frame_type=frame_type))


def mark_frames_as_good(raw_filenames):
    logger.info('Marking frames as good for filenames: {filenames}'.format(filenames=raw_filenames))
    for day_obs in DAYS_OBS:
        for filename in glob(os.path.join(DATA_ROOT, day_obs, 'processed', raw_filenames)):
            command = 'banzai_mark_frame_as_good --filename {filename} --db-address={db_address}'
            command = command.format(filename=os.path.basename(filename), db_address=os.environ['DB_ADDRESS'])
            os.system(command)
    logger.info('Finished marking frames as good for filenames: {filenames}'.format(filenames=raw_filenames))


def get_expected_number_of_calibrations(raw_filenames, calibration_type):
    number_of_stacks_that_should_have_been_created = 0
    for day_obs in DAYS_OBS:
        raw_filenames_for_this_dayobs = glob(os.path.join(DATA_ROOT, day_obs, 'raw', raw_filenames))
        if calibration_type.lower() == 'skyflat':
            # Group by filter
            observed_filters = []
            for raw_filename in raw_filenames_for_this_dayobs:
                skyflat_hdu = fits_utils.open_fits_file(raw_filename)
                observed_filters.append(skyflat_hdu[0].header['FILTER'])
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
    db_session = get_session(os.environ['DB_ADDRESS'])
    calibrations_in_db = db_session.query(CalibrationImage).filter(CalibrationImage.type == calibration_type)
    calibrations_in_db = calibrations_in_db.filter(CalibrationImage.is_master).all()
    db_session.close()
    assert number_of_stacks_that_should_have_been_created > 0
    assert len(calibrations_in_db) == number_of_stacks_that_should_have_been_created


@pytest.mark.e2e
@pytest.fixture(scope='module')
@mock.patch('banzai.dbs.requests.get', return_value=FakeResponse())
def init(configdb):
    create_db('.', db_address=os.environ['DB_ADDRESS'], configdb_address='http://configdbdev.lco.gtn/sites/')
    for instrument in INSTRUMENTS:
        populate_calibration_table_with_bpms(os.path.join(DATA_ROOT, instrument, 'bpm'),
                                             db_address=os.environ['DB_ADDRESS'])


@pytest.mark.e2e
@pytest.mark.master_bias
class TestMasterBiasCreation:
    @pytest.fixture(autouse=True)
    def stack_bias_frames(self, init):
        run_reduce_individual_frames('*b00.fits*')
        mark_frames_as_good('*b91.fits*')
        run_stack_calibrations('bias')

    def test_if_stacked_bias_frame_was_created(self):
        run_check_if_stacked_calibrations_were_created('*b00.fits*', 'bias')
        run_check_if_stacked_calibrations_are_in_db('*b00.fits*', 'BIAS')


@pytest.mark.e2e
@pytest.mark.master_dark
class TestMasterDarkCreation:
    @pytest.fixture(autouse=True)
    def stack_dark_frames(self):
        run_reduce_individual_frames('*d00.fits*')
        mark_frames_as_good('*d91.fits*')
        run_stack_calibrations('dark')

    def test_if_stacked_dark_frame_was_created(self):
        run_check_if_stacked_calibrations_were_created('*d00.fits*', 'dark')
        run_check_if_stacked_calibrations_are_in_db('*d00.fits*', 'DARK')


@pytest.mark.e2e
@pytest.mark.master_flat
class TestMasterFlatCreation:
    @pytest.fixture(autouse=True)
    def stack_flat_frames(self):
        run_reduce_individual_frames('*f00.fits*')
        mark_frames_as_good('*f91.fits*')
        run_stack_calibrations('skyflat')

    def test_if_stacked_flat_frame_was_created(self):
        run_check_if_stacked_calibrations_were_created('*f00.fits*', 'skyflat')
        run_check_if_stacked_calibrations_are_in_db('*f00.fits*', 'SKYFLAT')


@pytest.mark.e2e
@pytest.mark.science_files
class TestScienceFileCreation:
    @pytest.fixture(autouse=True)
    def reduce_science_frames(self):
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
