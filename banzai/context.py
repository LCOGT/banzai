import inspect


class Context(object):
    def __init__(self, args, **kwargs):
        constructor = namedtuple('Context', vars(args).keys())
        return constructor(**vars(args))

    def __init__(self, command_line_args, settings,
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

        for key, value in dict(inspect.getmembers(settings)).items():
            if not key.startswith('_'):
                super(PipelineContext, self).__setattr__(key, value)

        for keyword in vars(command_line_args):
            super(PipelineContext, self).__setattr__(keyword, getattr(command_line_args, keyword))

    def __delattr__(self, item):
        raise TypeError('Deleting attribute is not allowed. Context is immutable')

    def __setattr__(self, key, value):
        raise TypeError('Resetting attribute is not allowed. Context is immutable.')



