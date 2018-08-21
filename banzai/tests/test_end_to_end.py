import pytest
import os
from glob import glob

data_root = '/archive/engineering'

sites = [os.path.basename(site_path) for site_path in glob(os.path.join(data_root, '*'))]
instruments = [os.path.join(site, os.path.basename(instrument_path)) for site in sites
               for instrument_path in glob(os.path.join(os.path.join(data_root, site, '*')))]

days_obs = [os.path.join(instrument, os.path.basename(dayobs_path)) for instrument in instruments
            for dayobs_path in glob(os.path.join(data_root, instrument, '*'))]


def run_banzai(entry_point):
    for day_obs in days_obs:
        raw_path = os.path.join(data_root, day_obs, 'raw')
        command = '{cmd} --raw-path {raw_path} --fpack --db-address={db_address}'
        command = command.format(cmd=entry_point, raw_path=raw_path, db_address=os.environ['DB_ADDRESS'])
        os.system(command)


def test_if_stacked_calibrations_were_created(raw_filenames, calibration_type):
    number_of_stacks_that_should_have_been_created = 0
    created_stacked_calibrations = []
    for day_obs in days_obs:
        if len(glob(os.path.join(os.environ['NRES_DATA_ROOT'], day_obs, 'raw', raw_filenames))) > 0:
            number_of_stacks_that_should_have_been_created += 1
        created_stacked_calibrations += glob(os.path.join(os.environ['NRES_DATA_ROOT'], day_obs, 'specproc',
                                                          calibration_type.lower() + '*.fits*'))
    assert len(created_stacked_calibrations) == number_of_stacks_that_should_have_been_created


def test_if_stacked_calibrations_are_in_db(calibration_type):
    pass


@pytest.fixture(scope='module')
def init():
    create_db(os.environ['DB_URL'])
    populated_bpm()


@pytest.mark.master_bias
class TestMasterBiasCreation:
    @pytest.fixture(autouse=True)
    def stack_bias_frames(self, init):
        run_banzai('banzai_bias_maker')

    def test_if_stacked_bias_frame_was_created(self):
        test_if_stacked_calibrations_were_created('*b00.fits*', 'bias')
        test_if_stacked_calibrations_are_in_db('BIAS')


@pytest.mark.master_dark
class TestMasterDarkCreation:
    @pytest.fixture(autouse=True)
    def stack_dark_frames(self):
        run_banzai('banzai_dark_maker')

    def test_if_stacked_dark_frame_was_created(self):
        test_if_stacked_calibrations_were_created('*d00.fits*', 'dark')
        test_if_stacked_calibrations_are_in_db('DARK')


@pytest.mark.master_flat
class TestMasterFlatCreation:
    @pytest.fixture(autouse=True)
    def stack_flat_frames(self):
        run_banzai('banzai_flat_maker')

    def test_if_stacked_flat_frame_was_created(self):
        test_if_stacked_calibrations_were_created('*w00.fits*', 'flat')
        test_if_stacked_calibrations_are_in_db('FLAT')


@pytest.mark.science_files
class TestScienceFileCreation:
    @pytest.fixture(autouse=True)
    def reduce_science_frames(self):
        run_banzai('banzai_reduce_science_frames')

    def test_if_science_frames_were_created(self):
        expected_files = []
        created_files = []
        for day_obs in days_obs:
            expected_files += [os.path.basename(filename).replace('e00', 'e91')
                               for filename in glob(os.path.join(data_root, day_obs, 'raw', '*e00*'))]
            created_files += [os.path.basename(filename) for filename in glob(os.path.join(data_root, day_obs,
                                                                                           'processed', '*e90*'))]
        for expected_file in expected_files:
            assert expected_file in created_files
