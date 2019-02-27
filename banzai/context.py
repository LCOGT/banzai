from collections import namedtuple


def Context(args):
    if type(args) != 'dict':
        args_dict = vars(args)
    else:
        args_dict = args
    constructor = namedtuple('Context', args_dict.keys())
    return constructor(**args_dict)
