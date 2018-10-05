import logging
import multiprocessing
import sys
import os

import logutils.queue
from lcogt_logging import LCOGTFormatter

from banzai.utils import date_utils

queue = None
listener = None


class BanzaiLogger(logging.getLoggerClass()):
    def __init__(self, name, level='NOTSET'):
        super(BanzaiLogger, self).__init__(name, level)

    def _log(self, level, msg, *args, **kwargs):
        kwargs = _add_tag_dictionary(kwargs)
        super(BanzaiLogger, self)._log(level, msg, *args, **kwargs)


def _add_tag_dictionary(kwargs):
    tags = {}
    image = kwargs.pop('image', None)
    extra_tags = kwargs.pop('extra_tags', None)
    if image:
        tags.update(_image_to_tags(image))
    if extra_tags:
        tags.update(extra_tags)
    kwargs['extra'] = {'tags': tags}
    return kwargs


def _image_to_tags(image_config):
    tags = {'filename': os.path.basename(image_config.filename),
            'site': image_config.site,
            'instrument': image_config.instrument,
            'epoch': date_utils.epoch_date_to_string(image_config.epoch),
            'request_num': image_config.request_number}
    return tags


def start_logging(log_level='INFO', filename=None):
    logging.captureWarnings(True)
    # Set up the message queue
    global queue
    queue = multiprocessing.Queue(-1)

    # Start the listener process
    global listener
    listener = logutils.queue.QueueListener(queue)
    listener.start()

    # Set up the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, 'DEBUG'))

    # Set up the root handler
    if filename is not None:
        root_handler = logging.FileHandler(filename)
    else:
        root_handler = logging.StreamHandler(sys.stdout)
    formatter = LCOGTFormatter()
    root_handler.setFormatter(formatter)
    root_handler.setLevel(getattr(logging, log_level.upper(), None))
    root_logger.addHandler(root_handler)

    # Set upt he queue handler
    queue_handler = logutils.queue.QueueHandler(queue)
    queue_handler.setLevel(logging.DEBUG)
    root_logger.addHandler(queue_handler)


def stop_logging():
    if listener is not None:
        queue.put_nowait(None)
        listener.stop()
