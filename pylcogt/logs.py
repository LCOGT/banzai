from __future__ import absolute_import, print_function

import logutils.queue
import logging
import multiprocessing
import sys

def start_logging(log_level='INFO', filename=None):

    # Set up the message queue
    queue = multiprocessing.Queue(-1)
    queue.put_nowait(None)

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

    formatter = logging.Formatter('%(asctime)s %(processName)-10s %(name)s '
                                  '%(levelname)-8s %(message)s')
    root_handler.setFormatter(formatter)
    root_logger.addHandler(root_handler)

    queue_handler = logutils.queue.QueueHandler(queue)
    root_logger.addHandler(queue_handler)

    root_handler.setLevel(log_level)


def stop_logging():
    if listener is not None:
        listener.stop()
