from __future__ import absolute_import, division, print_function, unicode_literals
import hashlib
import os

from kombu import Connection, Queue, Exchange
from banzai import logs

__author__ = 'cmccully'

logger = logs.get_logger(__name__)


def post_to_archive_queue(image_path):
    def errback(exc, interval):
        logger.error('Error: %r', exc, exc_info=1)
        logger.info('Retry in %s seconds.', interval)
    fits_exchange = Exchange('fits_files', type='fanout')
    producer_queue = Queue('', fits_exchange, exclusive=True)
    with Connection.ensure_connection('amqp://guest:guest@cerberus.lco.gtn', max_retries=10,
                                      errback=errback) as conn:
        queue = conn.SimpleQueue(producer_queue)
        put = conn.ensure(queue, queue.put, max_retries=30, errback=errback)
        put({'path': image_path})


def make_output_directory(pipeline_context, image_config):
    # Create output directory if necessary
    output_directory = os.path.join(pipeline_context.processed_path, image_config.site,
                                    image_config.instrument, image_config.epoch)

    if pipeline_context.preview_mode:
        output_directory = os.path.join(output_directory, 'preview')
    else:
        output_directory = os.path.join(output_directory, 'processed')

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    return output_directory


def get_md5(filepath):
    with open(filepath, 'rb') as file:
        md5 = hashlib.md5(file.read()).hexdigest()
    return md5
