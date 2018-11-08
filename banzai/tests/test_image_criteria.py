from banzai.context import TelescopeCriterion
from banzai.utils.image_utils import image_passes_criteria
from collections import namedtuple
import operator
import mock

FakeTelescope = namedtuple('FakeTelescope', ['schedulable', 'camera_type'])


def test_telescope_criterion_should_fail():
    criterion = TelescopeCriterion('schedulable', operator.eq, True)
    assert not criterion.telescope_passes(FakeTelescope(schedulable=False, camera_type=None))


def test_telescope_criterion_should_pass():
    criterion = TelescopeCriterion('schedulable', operator.eq, True)
    assert criterion.telescope_passes(FakeTelescope(schedulable=True, camera_type=None))


def test_telescope_criterion_should_fail_with_exclude():
    criterion = TelescopeCriterion('camera_type', operator.contains, 'NRES', exclude=True)
    assert not criterion.telescope_passes(FakeTelescope(schedulable=True, camera_type='SciCam-NRES-01'))


def test_telescope_criterion_should_pass_with_exclude():
    criterion = TelescopeCriterion('camera_type', operator.contains, 'NRES', exclude=True)
    assert criterion.telescope_passes(FakeTelescope(schedulable=True, camera_type='SciCam'))


@mock.patch('banzai.dbs.get_telescope_for_file')
def test_image_passes_multiple_criteria_should_fail(mock_telescope):
    mock_telescope.return_value = FakeTelescope(schedulable=False, camera_type='SciCam')
    criteria = [TelescopeCriterion('schedulable', operator.eq, True),
                TelescopeCriterion('camera_type', operator.contains, 'NRES', exclude=True)]
    assert not image_passes_criteria('test.fits', criteria)


@mock.patch('banzai.dbs.get_telescope_for_file')
def test_image_passes_multiple_criteria_should_pass(mock_telescope):
    mock_telescope.return_value = FakeTelescope(schedulable=True, camera_type='SciCam')
    criteria = [TelescopeCriterion('schedulable', operator.eq, True),
                TelescopeCriterion('camera_type', operator.contains, 'NRES', exclude=True)]
    assert image_passes_criteria('test.fits', criteria)


@mock.patch('banzai.dbs.get_telescope_for_file')
def test_image_passes_multiple_criteria_should_fail_with_exclude(mock_telescope):
    mock_telescope.return_value = FakeTelescope(schedulable=True, camera_type='SciCam-NRES')
    criteria = [TelescopeCriterion('schedulable', operator.eq, True),
                TelescopeCriterion('camera_type', operator.contains, 'NRES', exclude=True)]
    assert not image_passes_criteria('test.fits', criteria)


@mock.patch('banzai.dbs.get_telescope_for_file')
def test_image_passes_multiple_criteria_should_pass_with_exclude(mock_telescope):
    mock_telescope.return_value = FakeTelescope(schedulable=True, camera_type='SciCam')
    criteria = [TelescopeCriterion('schedulable', operator.eq, True),
                TelescopeCriterion('camera_type', operator.contains, 'NRES', exclude=True)]
    assert image_passes_criteria('test.fits', criteria)
