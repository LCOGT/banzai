import os
from glob import glob
import argparse

import pytest

from banzai.dbs import populate_calibration_table_with_bpms, create_db, get_session, CalibrationImage
from banzai.utils import fits_utils

DATA_ROOT = os.path.join(os.sep, 'archive', 'engineering')

SITES = [os.path.basename(site_path) for site_path in glob(os.path.join(DATA_ROOT, '???'))]
INSTRUMENTS = [os.path.join(site, os.path.basename(instrument_path)) for site in SITES
               for instrument_path in glob(os.path.join(os.path.join(DATA_ROOT, site, '*')))]

DAYS_OBS = [os.path.join(instrument, os.path.basename(dayobs_path)) for instrument in INSTRUMENTS
            for dayobs_path in glob(os.path.join(DATA_ROOT, instrument, '201*'))]


def run_end_to_end_tests():
    parser = argparse.ArgumentParser()
    parser.add_argument('--marker', dest='marker', help='PyTest marker to run')
    parser.add_argument('--junit-file', dest='junit_file', help='Path to junit xml file with results')
    parser.add_argument('--code-path', dest='code_path', help='Path to directory with setup.py')
    args = parser.parse_args()
    os.chdir(args.code_path)
    command = 'python setup.py test -a "--durations=0 --junitxml={junit_file} -m {marker}"'
    os.system(command.format(junit_file=args.junit_file, marker=args.marker))


def run_banzai(entry_point):
    for day_obs in DAYS_OBS:
        raw_path = os.path.join(DATA_ROOT, day_obs, 'raw')
        command = '{cmd} --raw-path {raw_path} --fpack --db-address={db_address}'
        command = command.format(cmd=entry_point, raw_path=raw_path, db_address=os.environ['DB_ADDRESS'])
        os.system(command)


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
                                                          calibration_type.lower() + '*.fits*'))
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
def init():
    create_db('.', db_address=os.environ['DB_ADDRESS'], configdb_address='http://configdbdev.lco.gtn/sites/')
    for instrument in INSTRUMENTS:
        populate_calibration_table_with_bpms(os.path.join(DATA_ROOT, instrument, 'bpm'),
                                             db_address=os.environ['DB_ADDRESS'])


@pytest.mark.e2e
@pytest.mark.master_bias
class TestMasterBiasCreation:
    @pytest.fixture(autouse=True)
    def stack_bias_frames(self, init):
        run_banzai('banzai_bias_maker')

    def test_if_stacked_bias_frame_was_created(self):
        run_check_if_stacked_calibrations_were_created('*b00.fits*', 'bias')
        run_check_if_stacked_calibrations_are_in_db('*b00.fits*', 'BIAS')


@pytest.mark.e2e
@pytest.mark.master_dark
class TestMasterDarkCreation:
    @pytest.fixture(autouse=True)
    def stack_dark_frames(self):
        run_banzai('banzai_dark_maker')

    def test_if_stacked_dark_frame_was_created(self):
        run_check_if_stacked_calibrations_were_created('*d00.fits*', 'dark')
        run_check_if_stacked_calibrations_are_in_db('*d00.fits*', 'DARK')


@pytest.mark.e2e
@pytest.mark.master_flat
class TestMasterFlatCreation:
    @pytest.fixture(autouse=True)
    def stack_flat_frames(self):
        run_banzai('banzai_flat_maker')

    def test_if_stacked_flat_frame_was_created(self):
        run_check_if_stacked_calibrations_were_created('*f00.fits*', 'skyflat')
        run_check_if_stacked_calibrations_are_in_db('*f00.fits*', 'SKYFLAT')


@pytest.mark.e2e
@pytest.mark.science_files
class TestScienceFileCreation:
    @pytest.fixture(autouse=True)
    def reduce_science_frames(self):
        run_banzai('banzai_reduce_science_frames')

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
