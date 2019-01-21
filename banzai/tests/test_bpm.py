import pytest
import mock
import numpy as np

from banzai.bpm import BPMUpdater
from banzai.tests.utils import FakeImage, FakeContext


@pytest.fixture(scope='module')
def set_random_seed():
    np.random.seed(81232385)


class FakeImageForBPM(FakeImage):

    def __init__(self, *args, **kwargs):
        make_image_3d = kwargs.pop('make_image_3d', False)
        super(FakeImageForBPM, self).__init__(*args, **kwargs)
        self.header['DATASEC'] = '[1:{0},1:{1}]'.format(int(self.nx), int(self.ny))
        if make_image_3d:
            data_shape = (4, self.ny, self.nx)
        else:
            data_shape = (self.ny, self.nx)
        self.data = np.random.normal(1, 1, size=data_shape)
        self.header['SATURATE'] = 10000
        self.bpm = None
        self.instrument = FakeInstrument()


class FakeInstrument(object):
    def __init__(self):
        self.id = 1


class FakeContextForBPM(FakeContext):

    def __init__(self, *args, **kwargs):
        self.no_bpm = kwargs.pop('no_bpm', False)
        self.db_address = 'FakeDBAddress'
        super(FakeContextForBPM, self).__init__(*args, **kwargs)


def make_test_bpm(nx, ny, bad_pixel_fraction=0.1, make_3d=False):
    if make_3d:
        n_total = 4 * nx * ny
        final_shape = (4, ny, nx)
    else:
        n_total = nx * ny
        final_shape = (ny, nx)
    bpm = np.zeros(n_total, dtype=int)
    bad_pixels = np.random.choice(range(nx*ny), int(bad_pixel_fraction*n_total))
    bpm[bad_pixels] = 1
    return bpm.reshape(final_shape)


def test_no_input_images():
    tester = BPMUpdater(None)
    images = tester.do_stage([])
    assert len(images) == 0


@mock.patch('banzai.bpm.dbs.get_bpm_filename')
@mock.patch('banzai.bpm.load_bpm')
def test_adds_good_bpm(mock_load_bpm, mock_get_bpm_filename, set_random_seed):
    image = FakeImageForBPM()
    bpm_to_load = make_test_bpm(image.nx, image.ny)
    mock_get_bpm_filename.return_value = 'fake_bpm_filename'
    mock_load_bpm.return_value = bpm_to_load
    tester = BPMUpdater(FakeContextForBPM())
    image = tester.do_stage([image])[0]
    np.testing.assert_array_equal(image.bpm, bpm_to_load)
    assert image.header.get('L1IDMASK') == 'fake_bpm_filename'


@mock.patch('banzai.bpm.dbs.get_bpm_filename')
@mock.patch('banzai.bpm.load_bpm')
def test_adds_good_bpm_3d(mock_load_bpm, mock_get_bpm_filename, set_random_seed):
    image = FakeImageForBPM(make_image_3d=True)
    bpm_to_load = make_test_bpm(image.nx, image.ny, make_3d=True)
    mock_get_bpm_filename.return_value = 'fake_bpm_filename'
    mock_load_bpm.return_value = bpm_to_load
    tester = BPMUpdater(FakeContextForBPM())
    image = tester.do_stage([image])[0]
    np.testing.assert_array_equal(image.bpm, bpm_to_load)
    assert image.header.get('L1IDMASK') == 'fake_bpm_filename'


@mock.patch('banzai.bpm.dbs.get_bpm_filename')
def test_removes_image_if_file_missing(mock_get_bpm_filename):
    image = FakeImageForBPM()
    mock_get_bpm_filename.return_value = None
    tester = BPMUpdater(FakeContextForBPM())
    assert len(tester.do_stage([image])) == 0


@mock.patch('banzai.bpm.dbs.get_bpm_filename')
def test_uses_fallback_if_bpm_missing_and_no_bpm_set(mock_get_bpm_filename):
    image = FakeImageForBPM()
    fallback_bpm = np.zeros(image.data.shape, dtype=np.uint8)
    mock_get_bpm_filename.return_value = None
    tester = BPMUpdater(FakeContextForBPM(no_bpm=True))
    assert len(tester.do_stage([image])) == 1
    np.testing.assert_array_equal(image.bpm, fallback_bpm)


@mock.patch('banzai.bpm.dbs.get_bpm_filename')
@mock.patch('banzai.bpm.load_bpm')
def test_removes_image_if_wrong_shape(mock_load_bpm, mock_get_bpm_filename, set_random_seed):
    image = FakeImageForBPM()
    mock_get_bpm_filename.return_value = 'fake_bpm_filename'
    mock_load_bpm.return_value = make_test_bpm(image.nx+1, image.ny)
    tester = BPMUpdater(FakeContextForBPM())
    assert len(tester.do_stage([image])) == 0


@mock.patch('banzai.bpm.dbs.get_bpm_filename')
@mock.patch('banzai.bpm.load_bpm')
def test_removes_image_wrong_shape_3d(mock_load_bpm, mock_get_bpm_filename, set_random_seed):
    image = FakeImageForBPM(make_image_3d=True)
    mock_get_bpm_filename.return_value = 'fake_bpm_filename'
    mock_load_bpm.return_value = make_test_bpm(image.nx, image.ny)
    tester = BPMUpdater(FakeContextForBPM())
    assert len(tester.do_stage([image])) == 0
