from collections import namedtuple


def Context(args):
    constructor = namedtuple('Context', vars(args).keys())
    return constructor(**vars(args))
