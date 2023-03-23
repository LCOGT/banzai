import logging
import os
import traceback
import sys
import multiprocessing
from lcogt_logging import LCOGTFormatter

from banzai.utils import date_utils

logging.captureWarnings(True)
logging.basicConfig()
logger = logging.getLogger()
logger.handlers[0].setFormatter(LCOGTFormatter())

# Default the logger to INFO so that we actually get messages by default.
logger.setLevel('INFO')


class BanzaiLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        kwargs = _create_logging_tags_dictionary(kwargs)
        return msg, kwargs


def _create_logging_tags_dictionary(kwargs):
    try:
        tags = {}
        image = kwargs.pop('image', None)
        extra_tags = kwargs.pop('extra_tags', None)
        if image:
            tags.update(_image_to_tags(image))
        if extra_tags:
            tags.update(extra_tags)
        tags['processName'] = multiprocessing.current_process().name
        kwargs['extra'] = {'tags': tags}
    except Exception:
        logger = logging.getLogger('banzai')
        logger.error(format_exception())
        kwargs = {'extra': {'tags': {'error': 'Check implementation of this logging message'}}}
    return kwargs


def _image_to_tags(image):
    instrument = getattr(image, 'instrument', None)
    tags = {'filename': os.path.basename(getattr(image, 'filename', '')),
            'site': getattr(instrument, 'site', ''),
            'instrument': getattr(instrument, 'name', ''),
            'epoch': date_utils.epoch_date_to_string(getattr(image, 'epoch', '-')),
            'request_num': getattr(image, 'request_number', '-'),
            'obstype': getattr(image, 'obstype', ''),
            'filter': getattr(image, 'filter', '')}
    return tags


def set_log_level(log_level='INFO'):
    logger.setLevel(log_level.upper())


def format_exception():
    exc_type, exc_value, exc_tb = sys.exc_info()
    return traceback.format_exception(exc_type, exc_value, exc_tb)


def get_logger():
    return BanzaiLogger(logging.getLogger('banzai'))
