import operator


class InstrumentCriterion:
    def __init__(self, attribute, comparison_operator, comparison_value):
        self.attribute = attribute
        if 'not' in comparison_operator:
            self.exclude = True
            comparison_operator = comparison_operator.replace('not', '').strip()
            self.comparison_operator = getattr(operator, comparison_operator)
        else:
            self.exclude = False

        self.comparison_operator = getattr(operator, comparison_operator)
        self.comparison_value = comparison_value

    def instrument_passes(self, instrument):
        test = self.comparison_operator(getattr(instrument, self.attribute), self.comparison_value)
        if self.exclude:
            test = not test
        return test

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


def instrument_passes_criteria(instrument, criteria):
    passes = True
    for args in criteria:
        criterion = InstrumentCriterion(*args)
        if not criterion.instrument_passes(instrument):
            passes = False
    return passes
