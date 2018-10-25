class TelescopeCriterion:
    def __init__(self, attribute, comparison_operator, comparison_value, exclude=False):
        self.attribute = attribute
        self.comparison_value = comparison_value
        self.exclude = exclude
        self.comparison_operator = comparison_operator

    def telescope_passes(self, telescope):
        test = self.comparison_operator(getattr(telescope, self.attribute), self.comparison_value)
        if self.exclude:
            test = not test
        return test


class PipelineContext(object):
    def __init__(self, command_line_args, allowed_instrument_criteria, processed_path='/archive/engineering/',
                 post_to_archive=False, fpack=True, rlevel=91, db_address='mysql://cmccully:password@localhost/test',
                 log_level='INFO', preview_mode=False, max_tries=5, post_to_elasticsearch=False,
                 elasticsearch_url='http://elasticsearch.lco.gtn:9200', elasticsearch_doc_type='qc',
                 elasticsearch_qc_index='banzai_qc', **kwargs):
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

        for keyword in vars(command_line_args):
            super(PipelineContext, self).__setattr__(keyword, getattr(command_line_args, keyword))

    def __delattr__(self, item):
        raise TypeError('Deleting attribute is not allowed. PipelineContext is immutable')

    def __setattr__(self, key, value):
        raise TypeError('Resetting attribute is not allowed. PipelineContext is immutable.')
