from __future__ import absolute_import, print_function, division

import logutils.queue
import logging
import multiprocessing
import sys
from lcogt_logging import LCOGTFormatter
from .utils import date_utils

def get_logger(name):
    logger = logging.getLogger(name)
    # This allows us to control the logging level with the root logger.
    logger.setLevel(logging.DEBUG)
    return logger


def start_logging(log_level='INFO', filename=None):

    # Set up the message queue
    global queue
    queue = multiprocessing.Queue(-1)

    # Start the listener process
    global listener
    listener = logutils.queue.QueueListener(queue)
    listener.start()

    # Set up the logging format
    root_logger = logging.getLogger()
    if filename is not None:
        root_handler = logging.FileHandler(filename)
    else:
        root_handler = logging.StreamHandler(sys.stdout)

    def get_process_name():
        return multiprocessing.current_process().name

    formatter = LCOGTFormatter(extra_tags={'processName': get_process_name})
    root_handler.setFormatter(formatter)
    root_handler.setLevel(getattr(logging, log_level.upper(), None))
    root_logger.addHandler(root_handler)

    queue_handler = logutils.queue.QueueHandler(queue)

    queue_handler.setLevel(logging.DEBUG)
    root_logger.addHandler(queue_handler)


def stop_logging():
    if listener is not None:
        queue.put_nowait(None)
        listener.stop()


def image_config_to_tags(image_config, group_by_keywords):
    tags = {'tags': {'site': image_config.site,
                     'instrument': image_config.instrument,
                     'epoch': date_utils.epoch_date_to_string(image_config.epoch)}}
    if group_by_keywords is not None:
        for i, keyword in enumerate(group_by_keywords):
            tags['tags'][keyword] = getattr(image_config, keyword)

    return tags


def add_tag(tags, keyword, value):
    tags['tags'][keyword] = value


def pop_tag(tags, keyword):
    tags['tags'].pop(keyword)