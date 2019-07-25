class Context:
    def __init__(self, args):
        if type(args) != dict:
            args_dict = vars(args)
        else:
            args_dict = args
        for key in args_dict:
            super(Context, self).__setattr__(key, args_dict[key])

    def __delattr__(self, item):
        raise TypeError('Deleting attribute is not allowed. PipelineContext is immutable')

    def __setattr__(self, key, value):
        raise TypeError('Resetting attribute is not allowed. PipelineContext is immutable.')
