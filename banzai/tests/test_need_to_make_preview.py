import mock
from banzai.dbs import TelescopeMissingException
from banzai.preview import need_to_make_preview
from banzai.context import TelescopeCriterion
import operator

md5_hash1 = '49a6bb35cdd3859224c0214310b1d9b6'
md5_hash2 = 'aec5ef355e7e43a59fedc88ac95caed6'


class FakeTelescope(object):
    def __init__(self, schedulable=True):
        self.schedulable = schedulable


class FakePreviewImage(object):
    def __init__(self, success=False, checksum=md5_hash1, tries=0):
        self.success = success
        self.checksum = checksum
        self.tries = tries


@mock.patch('banzai.dbs.get_telescope_for_file')
def test_no_preview_if_telescope_is_not_schedulable(mock_telescope):
    mock_telescope.return_value = FakeTelescope(schedulable=False)
    assert not need_to_make_preview('test.fits', [TelescopeCriterion('schedulable', operator.eq, True)])


@mock.patch('banzai.dbs.get_telescope_for_file')
def test_no_preview_if_telescope_not_in_db(mock_telescope):
    mock_telescope.return_value = FakeTelescope(schedulable=True)
    mock_telescope.side_effect = TelescopeMissingException
    assert not need_to_make_preview('test.fits', [])


@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_preview_image')
@mock.patch('banzai.dbs.get_telescope_for_file')
def test_no_preview_if_previous_success(mock_telescope, mock_preview, mock_md5):
    mock_telescope.return_value = FakeTelescope(schedulable=True)
    mock_preview.return_value = FakePreviewImage(success=True, checksum=md5_hash1)
    mock_md5.return_value = md5_hash1
    assert not need_to_make_preview('test.fits', [])


@mock.patch('banzai.dbs.commit_preview_image')
@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_preview_image')
@mock.patch('banzai.dbs.get_telescope_for_file')
def test_preview_if_never_tried(mock_telescope, mock_preview, mock_md5, mock_commit):
    mock_telescope.return_value = FakeTelescope(schedulable=True)
    mock_preview.return_value = FakePreviewImage(success=False, checksum=md5_hash1, tries=0)
    mock_md5.return_value = md5_hash1
    assert need_to_make_preview('test.fits', [])


@mock.patch('banzai.dbs.commit_preview_image')
@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_preview_image')
@mock.patch('banzai.dbs.get_telescope_for_file')
def test_preview_if_tries_less_than_max(mock_telescope, mock_preview, mock_md5, mock_commit):
    mock_telescope.return_value = FakeTelescope(schedulable=True)
    mock_preview.return_value = FakePreviewImage(success=False, checksum=md5_hash1, tries=3)
    mock_md5.return_value = md5_hash1
    assert need_to_make_preview('test.fits', [], max_tries=5)


@mock.patch('banzai.dbs.commit_preview_image')
@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_preview_image')
@mock.patch('banzai.dbs.get_telescope_for_file')
def test_no_preview_if_tries_at_max(mock_telescope, mock_preview, mock_md5, mock_commit):
    max_tries = 5
    mock_telescope.return_value = FakeTelescope(schedulable=True)
    mock_preview.return_value = FakePreviewImage(success=False, checksum=md5_hash1, tries=max_tries)
    mock_md5.return_value = md5_hash1
    assert not need_to_make_preview('test.fits', [], max_tries=max_tries)


@mock.patch('banzai.dbs.commit_preview_image')
@mock.patch('banzai.utils.file_utils.get_md5')
@mock.patch('banzai.dbs.get_preview_image')
@mock.patch('banzai.dbs.get_telescope_for_file')
def test_preview_if_new_checksum(mock_telescope, mock_preview, mock_md5, mock_commit):
    # assert that tries and success are reset to 0
    mock_telescope.return_value = FakeTelescope(schedulable=True)
    preview_image = FakePreviewImage(success=True, checksum=md5_hash1, tries=3)
    mock_preview.return_value = preview_image
    mock_md5.return_value = md5_hash2
    assert need_to_make_preview('test.fits', [])
    assert not preview_image.success
    assert preview_image.tries == 0
    assert preview_image.checksum == md5_hash2
