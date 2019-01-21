from collections import namedtuple
import operator

import mock

from banzai.context import InstrumentCriterion
from banzai.utils.image_utils import image_passes_criteria

FakeInstrument = namedtuple('FakeInstrument', ['schedulable', 'type'])


def test_instrument_criterion_should_fail():
    criterion = InstrumentCriterion('schedulable', operator.eq, True)
    assert not criterion.instrument_passes(FakeInstrument(schedulable=False, type=None))


def test_instrument_criterion_should_pass():
    criterion = InstrumentCriterion('schedulable', operator.eq, True)
    assert criterion.instrument_passes(FakeInstrument(schedulable=True, type=None))


def test_instrument_criterion_should_fail_with_exclude():
    criterion = InstrumentCriterion('type', operator.contains, 'NRES', exclude=True)
    assert not criterion.instrument_passes(FakeInstrument(schedulable=True, type='SciCam-NRES-01'))


def test_instrument_criterion_should_pass_with_exclude():
    criterion = InstrumentCriterion('type', operator.contains, 'NRES', exclude=True)
    assert criterion.instrument_passes(FakeInstrument(schedulable=True, type='SciCam'))


@mock.patch('banzai.dbs.get_instrument_for_file')
def test_image_passes_multiple_criteria_should_fail(mock_instrument):
    mock_instrument.return_value = FakeInstrument(schedulable=False, type='SciCam')
    criteria = [InstrumentCriterion('schedulable', operator.eq, True),
                InstrumentCriterion('type', operator.contains, 'NRES', exclude=True)]
    assert not image_passes_criteria('test.fits', criteria)


@mock.patch('banzai.dbs.get_instrument_for_file')
def test_image_passes_multiple_criteria_should_pass(mock_instrument):
    mock_instrument.return_value = FakeInstrument(schedulable=True, type='SciCam')
    criteria = [InstrumentCriterion('schedulable', operator.eq, True),
                InstrumentCriterion('type', operator.contains, 'NRES', exclude=True)]
    assert image_passes_criteria('test.fits', criteria)


@mock.patch('banzai.dbs.get_instrument_for_file')
def test_image_passes_multiple_criteria_should_fail_with_exclude(mock_instrument):
    mock_instrument.return_value = FakeInstrument(schedulable=True, type='SciCam-NRES')
    criteria = [InstrumentCriterion('schedulable', operator.eq, True),
                InstrumentCriterion('type', operator.contains, 'NRES', exclude=True)]
    assert not image_passes_criteria('test.fits', criteria)


@mock.patch('banzai.dbs.get_instrument_for_file')
def test_image_passes_multiple_criteria_should_pass_with_exclude(mock_instrument):
    mock_instrument.return_value = FakeInstrument(schedulable=True, type='SciCam')
    criteria = [InstrumentCriterion('schedulable', operator.eq, True),
                InstrumentCriterion('type', operator.contains, 'NRES', exclude=True)]
    assert image_passes_criteria('test.fits', criteria)
