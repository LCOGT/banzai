from __future__ import absolute_import, division, print_function, unicode_literals

from copy import copy


class PipelineContext(object):
    processed_path = '/archive/engineering'
    raw_path = '/archive/engineering'
    post_to_archive = False
    fpack = True
    rlevel = 91
    db_address = 'mysql://cmccully:password@localhost/test'
    log_level = 'DEBUG'
    preview_mode = False
    filename = None
    max_preview_tries = 5

    def __init__(self, args):
        args_dict = vars(args)
        for key in args_dict.keys():
            setattr(self, key, args_dict[key])

    def copy(self):
        return copy(self)
