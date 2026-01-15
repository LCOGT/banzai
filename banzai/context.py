class Context:
    def __init__(self, args):
        if type(args) != dict:
            args_dict = vars(args)
        else:
            args_dict = args

        # If a separate calibration db address is not provided, fall back to using the primary db address
        if 'cal_db_address' not in args_dict or args_dict.get('cal_db_address') is None:
            if 'db_address' in args_dict:
                args_dict['cal_db_address'] = args_dict['db_address']

        for key in args_dict:
            super(Context, self).__setattr__(key, args_dict[key])

    def __delattr__(self, item):
        raise TypeError('Deleting attribute is not allowed. PipelineContext is immutable')

    def __setattr__(self, key, value):
        raise TypeError('Resetting attribute is not allowed. PipelineContext is immutable.')
