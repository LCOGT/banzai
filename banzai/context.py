import inspect
from banzai.utils import image_utils
from banzai import dbs
import json
import logging

logger = logging.getLogger(__name__)


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


def instrument_passes_criteria(instrument, criteria):
    passes = True
    for criterion in criteria:
        if not criterion.instrument_passes(instrument):
            passes = False
    return passes


class PipelineContext(object):
    def __init__(self, command_line_args=None, settings=None,
                 processed_path='/archive/engineering/', post_to_archive=False, fpack=True, rlevel=91,
                 db_address='mysql://cmccully:password@localhost/test', log_level='INFO', preview_mode=False,
                 max_tries=5, post_to_elasticsearch=False, elasticsearch_url='http://elasticsearch.lco.gtn:9200',
                 elasticsearch_doc_type='qc', elasticsearch_qc_index='banzai_qc', realtime_reduction=False, **kwargs):
        # TODO: preview_mode will be removed once we start processing everything in real time.
        # TODO: no_bpm can also be removed once we are in "do our best" mode
        local_variables = locals()
        for variable in local_variables:
            if variable == 'kwargs':
                kwarg_variables = local_variables[variable]
                for kwarg in kwarg_variables:
                    super(PipelineContext, self).__setattr__(kwarg, kwarg_variables[kwarg])
            elif variable != 'command_line_args':
                super(PipelineContext, self).__setattr__(variable, local_variables[variable])

        if settings is not None:
            for key, value in dict(inspect.getmembers(settings)).items():
                if not key.startswith('_'):
                    super(PipelineContext, self).__setattr__(key, value)

        if command_line_args is not None:
            for keyword in vars(command_line_args):
                super(PipelineContext, self).__setattr__(keyword, getattr(command_line_args, keyword))

    def __delattr__(self, item):
        raise TypeError('Deleting attribute is not allowed. PipelineContext is immutable')

    def __setattr__(self, key, value):
        raise TypeError('Resetting attribute is not allowed. PipelineContext is immutable.')

    def image_can_be_processed(self, header):
        instrument = dbs.get_instrument(header, db_address=self.db_address)
        passes = instrument_passes_criteria(instrument, self.FRAME_SELECTION_CRITERIA)
        passes &= image_utils.get_obstype(header) in self.LAST_STAGE
        passes &= image_utils.get_reduction_level(header) == '00'
        return passes

    def to_json(self):
        json_dict = self.__dict__.copy()
        json_dict.pop('self')
        json_dict.pop('settings')
        logger.info(json_dict)
        return json.dumps(json_dict)

    @classmethod
    def from_dict(cls, json_dict):
        return cls(**json_dict)
