import logging
import os
import traceback
import sys

from banzai.utils import date_utils

logger = logging.getLogger(__name__)


class BanzaiLogger(logging.getLoggerClass()):
    def __init__(self, name, level='NOTSET'):
        super(BanzaiLogger, self).__init__(name, level)

    def _log(self, level, msg, *args, **kwargs):
        kwargs = _create_logging_tags_dictionary(kwargs)
        super(BanzaiLogger, self)._log(level, msg, *args, **kwargs)


def _create_logging_tags_dictionary(kwargs):
    try:
        tags = {}
        image = kwargs.pop('image', None)
        extra_tags = kwargs.pop('extra_tags', None)
        if image:
            tags.update(_image_to_tags(image))
        if extra_tags:
            tags.update(extra_tags)
        kwargs['extra'] = {'tags': tags}
    except Exception:
        logger.error(format_exception())
        kwargs = {'extra': {'tags': {'error': 'Check implementation of this logging message'}}}
    return kwargs


def _image_to_tags(image_config):
    tags = {'filename': os.path.basename(image_config.filename),
            'site': image_config.site,
            'instrument': image_config.camera,
            'epoch': date_utils.epoch_date_to_string(image_config.epoch),
            'request_num': image_config.request_number,
            'obstype': image_config.obstype,
            'filter': image_config.filter}
    return tags


def set_log_level(log_level='INFO'):
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level.upper())


def format_exception():
    exc_type, exc_value, exc_tb = sys.exc_info()
    return traceback.format_exception(exc_type, exc_value, exc_tb)
