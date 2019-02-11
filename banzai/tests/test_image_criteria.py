from collections import namedtuple
import operator

import mock

from banzai.context import InstrumentCriterion, instrument_passes_criteria

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


def test_image_passes_multiple_criteria_should_fail():
    criteria = [InstrumentCriterion('schedulable', operator.eq, True),
                InstrumentCriterion('type', operator.contains, 'NRES', exclude=True)]
    assert not instrument_passes_criteria(FakeInstrument(schedulable=False, type='SciCam'), criteria)


def test_image_passes_multiple_criteria_should_pass():
    criteria = [InstrumentCriterion('schedulable', operator.eq, True),
                InstrumentCriterion('type', operator.contains, 'NRES', exclude=True)]
    assert instrument_passes_criteria(FakeInstrument(schedulable=True, type='SciCam'), criteria)


def test_image_passes_multiple_criteria_should_fail_with_exclude():
    criteria = [InstrumentCriterion('schedulable', operator.eq, True),
                InstrumentCriterion('type', operator.contains, 'NRES', exclude=True)]
    assert not instrument_passes_criteria(FakeInstrument(schedulable=True, type='SciCam-NRES'), criteria)


def test_image_passes_multiple_criteria_should_pass_with_exclude():
    criteria = [InstrumentCriterion('schedulable', operator.eq, True),
                InstrumentCriterion('type', operator.contains, 'NRES', exclude=True)]
    assert instrument_passes_criteria(FakeInstrument(schedulable=True, type='SciCam'), criteria)
