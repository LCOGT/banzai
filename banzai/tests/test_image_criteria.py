import pytest
from collections import namedtuple

from banzai.utils.instrument_utils import InstrumentCriterion, instrument_passes_criteria

pytestmark = pytest.mark.image_criteria

FakeInstrument = namedtuple('FakeInstrument', ['schedulable', 'type'])


def test_instrument_criterion_should_fail():
    criterion = InstrumentCriterion('schedulable', 'eq', True)
    assert not criterion.instrument_passes(FakeInstrument(schedulable=False, type=None))


def test_instrument_criterion_should_pass():
    criterion = InstrumentCriterion('schedulable', 'eq', True)
    assert criterion.instrument_passes(FakeInstrument(schedulable=True, type=None))


def test_instrument_criterion_should_fail_with_exclude():
    criterion = InstrumentCriterion('type', 'not contains', 'NRES')
    assert not criterion.instrument_passes(FakeInstrument(schedulable=True, type='SciCam-NRES-01'))


def test_instrument_criterion_should_pass_with_exclude():
    criterion = InstrumentCriterion('type', 'not contains', 'NRES')
    assert criterion.instrument_passes(FakeInstrument(schedulable=True, type='SciCam'))


def test_image_passes_multiple_criteria_should_fail():
    criteria = [('schedulable', 'eq', True), ('type', 'not contains', 'NRES')]
    assert not instrument_passes_criteria(FakeInstrument(schedulable=False, type='SciCam'), criteria)


def test_image_passes_multiple_criteria_should_pass():
    criteria = [('schedulable', 'eq', True), ('type', 'not contains', 'NRES')]
    assert instrument_passes_criteria(FakeInstrument(schedulable=True, type='SciCam'), criteria)


def test_image_passes_multiple_criteria_should_fail_with_exclude():
    criteria = [('schedulable', 'eq', True), ('type', 'not contains', 'NRES')]
    assert not instrument_passes_criteria(FakeInstrument(schedulable=True, type='SciCam-NRES'), criteria)


def test_image_passes_multiple_criteria_should_pass_with_exclude():
    criteria = [('schedulable', 'eq', True), ('type', 'not contains', 'NRES')]
    assert instrument_passes_criteria(FakeInstrument(schedulable=True, type='SciCam'), criteria)
