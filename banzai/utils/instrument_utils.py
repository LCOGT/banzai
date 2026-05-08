import operator
import traceback
from banzai.utils import import_utils
from banzai import logs


logger = logs.get_logger()

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


def get_processing_queue(message_body, runtime_context):
    try:
        factory = import_utils.import_attribute(runtime_context.FRAME_FACTORY)
        instrument = factory.get_instrument_from_header(message_body, runtime_context.db_address)
    except Exception:
        logger.error(f'Could not get instrument from header. {traceback.format_exc()}', extra_tags={'filename': message_body['filename']})
        raise

    if instrument is None or instrument.nx is None:
        queue_name = runtime_context.CELERY_TASK_QUEUE_NAME
    elif instrument.nx * instrument.ny > runtime_context.LARGE_WORKER_THRESHOLD:
        queue_name = runtime_context.LARGE_WORKER_QUEUE
    else:
        queue_name = runtime_context.CELERY_TASK_QUEUE_NAME
    return queue_name
