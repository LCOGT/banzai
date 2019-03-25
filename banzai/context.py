import json

from collections import namedtuple
from dramatiq import JSONEncoder


def Context(args):
    if type(args) != dict:
        args_dict = vars(args)
    else:
        args_dict = args
    constructor = namedtuple('Context', args_dict.keys())
    return constructor(**args_dict)


class ContextJSONEncoder(JSONEncoder):
    def encode(self, data):
        return json.dumps(data, separators=(",", ":"), default=str).encode("utf-8")
