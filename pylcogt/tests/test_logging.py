__author__ = 'cmccully'

from .. import logs
import logutils
import logutils.queue
import logging
import logging.handlers
import multiprocessing
# Arrays used for random selections in this demo

LEVELS = [logging.DEBUG, logging.INFO, logging.WARNING,
          logging.ERROR, logging.CRITICAL]

LOGGERS = ['a.b.c', 'd.e.f']

MESSAGES = [
    'Random message #1',
    'Random message #2',
    'Random message #3',
]

# This is the worker process top-level loop, which just logs ten events with
# random intervening delays before terminating.
# The print messages are just so you know it's doing something!
def worker_process():
    name = multiprocessing.current_process().name
    print('Worker started: %s' % name)
    import time
    from numpy.random import choice, random
    for i in range(10):
        time.sleep(random())
        logger = logs.get_logger(choice(LOGGERS))
        level = choice(LEVELS)
        message = choice(MESSAGES)
        logger.log(level, message)
    print('Worker finished: %s' % name)

# Here's where the demo gets orchestrated. Create the queue, create and start
# the listener, create ten workers and start them, wait for them to finish,
# then send a None to the queue to tell the listener to finish.
def test_logging():

    logs.start_logging(filename='/tmp/mptest.log')

    workers = []
    for i in range(10):
        worker = multiprocessing.Process(target=worker_process)
        workers.append(worker)
        worker.start()
    for w in workers:
        w.join()

    logs.stop_logging()

    logfile = open('/tmp/mptest.log')

    file_length = sum(1 for line in logfile)

    logfile.close()
    assert file_length == 100

if __name__ == '__main__':
    test_logging()