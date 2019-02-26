from banzai import settings


class InstrumentCriterion:
    def __init__(self, attribute, comparison_operator, comparison_value, exclude=False):
        self.attribute = attribute
        self.comparison_operator = comparison_operator
        self.comparison_value = comparison_value
        self.exclude = exclude

    def instrument_passes(self, instrument):
        test = self.comparison_operator(getattr(instrument, self.attribute), self.comparison_value)
        if self.exclude:
            test = not test
        return test

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


def instrument_passes_criteria(instrument, ignore_schedulability):
    passes = True
    for criterion in settings.FRAME_SELECTION_CRITERIA:
        if not criterion.instrument_passes(instrument):
            passes = False
    if not ignore_schedulability:
        settings.SchedulableCriterion.instrument_passes(instrument)
    return passes
