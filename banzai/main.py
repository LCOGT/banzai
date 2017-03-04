""" main.py: Main driver script for banzai.

    The main() function is a console entry point.

Author
    Curtis McCully (cmccully@lcogt.net)

October 2015
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import argparse
import multiprocessing
import os
import logging

from kombu import Connection, Queue, Exchange
from kombu.mixins import ConsumerMixin

import banzai.images
from banzai.stages import get_stages_todo
from banzai.utils import image_utils, date_utils
from banzai import logs
from banzai import dbs


logger = logging.get_logger('banzai')


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


def parse_end_of_night_command_line_arguments():
    parser = argparse.ArgumentParser(
        description='Make master calibration frames from LCOGT imaging data.')
    parser.add_argument("--raw-path", dest='raw_path', default='/archive/engineering',
                        help='Top level directory where the raw data is stored')
    parser.add_argument("--processed-path", default='/archive/engineering/',
                        help='Top level directory where the processed data will be stored')
    parser.add_argument("--log-level", default='debug', choices=['debug', 'info', 'warning',
                                                                 'critical', 'fatal', 'error'])
    parser.add_argument('--post-to-archive', dest='post_to_archive', action='store_true',
                        default=False)
    parser.add_argument('--db-address', dest='db_address',
                        default='mysql://cmccully:password@localhost/test',
                        help='Database address: Should be in SQLAlchemy form')
    parser.add_argument('--fpack', dest='fpack', action='store_true', default=False,
                        help='Fpack the output files?')
    parser.add_argument('--rlevel', dest='rlevel', default=91, help='Reduction level')

    parser.add_argument('--filename', dest='filename', default=None,
                        help='Filename of the image to reduce.')
    args = parser.parse_args()

    return PipelineContext(args)


def run(stages_to_do, pipeline_context, image_types=[], calibration_maker=False, log_message=''):
    """
    Main driver script for banzai.
    """
    if len(log_message) > 0:
        logger.info(log_message, extra={'tags': {'raw_path': pipeline_context.raw_path}})

    image_list = image_utils.make_image_list(pipeline_context)
    image_list = image_utils.select_images(image_list, image_types)
    images = banzai.images.read_images(image_list, pipeline_context)

    for stage in stages_to_do:
        stage_to_run = stage(pipeline_context)
        images = stage_to_run.run(images)

    output_files = image_utils.save_images(pipeline_context, images,
                                           master_calibration=calibration_maker)
    return output_files


def run_from_console(last_stage_todo=None, extra_stages=[],
                     image_types=[], calibration_maker=False, log_message=''):
    pipeline_context = parse_end_of_night_command_line_arguments()
    logs.set_level(log_level=pipeline_context.log_level)
    stages_todo = get_stages_todo(last_stage=last_stage_todo, extra_stages=extra_stages)
    run(stages_todo, pipeline_context, image_types=image_types, calibration_maker=calibration_maker,
        log_message=log_message)


def reduce_frames_one_by_one(last_stage_todo=None, image_types=['EXPOSE', 'STANDARD']):
    pipeline_context = parse_end_of_night_command_line_arguments()
    logs.set_level(log_level=pipeline_context.log_level)
    stages_todo = get_stages_todo(last_stage_todo=last_stage_todo)
    image_list = image_utils.make_image_list(pipeline_context)
    original_filename = pipeline_context.filename

    for image in image_list:
        pipeline_context.filename = os.path.basename(image)
        try:
            run(stages_todo, pipeline_context, image_types=image_types)
        except Exception as e:
            logger.error('{0}'.format(e), extra={'tags': {'filename': pipeline_context.filename,
                                                          'filepath': pipeline_context.raw_path}})
    pipeline_context.filename = original_filename


def make_master_bias():
    run_from_console(last_stage_todo='trim.Trimmer', extra_stages=['bias.BiasMaker'],
                     image_types=['BIAS'], calibration_maker=True, log_message='Making Master BIAS')


def make_master_dark():
    run_from_console(last_stage_todo='bias.BiasSubtractor', extra_stages=['dark.DarkMaker'],
                     image_types=['DARK'], calibration_maker=True, log_message='Making Master Dark')


def make_master_flat():
    run_from_console(last_stage_todo='dark.DarkSubtractor', extra_stages=['flats.FlatMaker'],
                     image_types=['SKYFLAT'], calibration_maker=True,
                     log_message='Making Master Flat')


def reduce_science_frames():
    reduce_frames_one_by_one()


def reduce_experimental_frames(pipeline_context):
    reduce_frames_one_by_one(image_types=['EXPERIMENTAL'])


def reduce_trailed_frames(pipeline_context):
    reduce_frames_one_by_one(image_types=['TRAILED'])


def preprocess_sinistro_frames(pipeline_context):
    reduce_frames_one_by_one(last_stage_todo='mosaic.MosaicCreator',
                             image_types=['EXPOSE', 'STANDARD', 'BIAS', 'DARK', 'SKYFLAT',
                                          'TRAILED', 'EXPERIMENTAL'])


def reduce_night():
    parser = argparse.ArgumentParser(
        description='Reduce all the data from a site at the end of a night.')
    parser.add_argument('--site', dest='site', help='Site code (e.g. ogg)')
    parser.add_argument('--dayobs', dest='dayobs',
                        default=None, help='Day-Obs to reduce (e.g. 20160201)')
    parser.add_argument('--raw-path-root', dest='rawpath_root', default='/archive/engineering',
                        help='Top level directory with raw data.')
    parser.add_argument("--processed-path", default='/archive/engineering',
                        help='Top level directory where the processed data will be stored')

    parser.add_argument("--log-level", default='debug', choices=['debug', 'info', 'warning',
                                                                 'critical', 'fatal', 'error'])
    parser.add_argument('--post-to-archive', dest='post_to_archive', action='store_true',
                        default=False)
    parser.add_argument('--fpack', dest='fpack', action='store_true', default=False,
                        help='Fpack the output files?')

    parser.add_argument('--rlevel', dest='rlevel', default=91, help='Reduction level')
    parser.add_argument('--db-address', dest='db_address',
                        default='mysql://cmccully:password@localhost/test',
                        help='Database address: Should be in SQLAlchemy form')

    args = parser.parse_args()

    pipeline_context = PipelineContext(args)

    logs.start_logging(log_level=pipeline_context.log_level)

    # Ping the configdb to get currently schedulable telescopes
    try:
        dbs.populate_telescope_tables(db_address=pipeline_context.db_address)
    except Exception as e:
        logger.error('Could not connect to the configdb.')
        logger.error(e)

    timezone = dbs.get_timezone(args.site, db_address=args.db_address)

    telescopes = dbs.get_schedulable_telescopes(args.site, db_address=args.db_address)

    if timezone is not None:
        # If no dayobs is given, calculate it.
        if args.dayobs is None:
            args.dayobs = date_utils.get_dayobs(timezone=timezone)

        # For each telescope at the given site
        for telescope in telescopes:
            pipeline_context.raw_path = os.path.join(args.rawpath_root, args.site,
                                                     telescope.instrument, args.dayobs, 'raw')
            try:
                # Run the reductions on the given dayobs
                make_master_bias(pipeline_context)
                make_master_dark(pipeline_context)
                make_master_flat(pipeline_context)
                reduce_science_frames(pipeline_context)
            except Exception as e:
                logger.error(e)
    logs.stop_logging()

def run_preview_pipeline():
    parser = argparse.ArgumentParser(
        description='Make master calibration frames from LCOGT imaging data.')

    parser.add_argument("--processed-path", default='/archive/engineering',
                        help='Top level directory where the processed data will be stored')
    parser.add_argument("--log-level", default='debug', choices=['debug', 'info', 'warning',
                                                                 'critical', 'fatal', 'error'])
    parser.add_argument('--post-to-archive', dest='post_to_archive', action='store_true',
                        default=False)
    parser.add_argument('--db-address', dest='db_address',
                        default='mysql://cmccully:password@localhost/test',
                        help='Database address: Should be in SQLAlchemy form')
    parser.add_argument('--fpack', dest='fpack', action='store_true', default=False,
                        help='Fpack the output files?')
    parser.add_argument('--rlevel', dest='rlevel', default=11, help='Reduction level')

    parser.add_argument('--n-processes', dest='n_processes', default=12,
                        help='Number of listener processes to spawn.', type=int)

    parser.add_argument('--broker-url', dest='broker_url',
                        default='amqp://guest:guest@rabbitmq.lco.gtn:5672//?heartbeat=10',
                        help='URL for the broker service.')
    parser.add_argument('--queue-name', dest='queue_name', default='preview_pipeline',
                        help='Name of the queue to listen to from the fits exchange.')
    parser.add_argument('--max-preview-tries', dest='max_preview_tries', default=5,
                        help='Maximum number of tries to produce a preview image.')
    args = parser.parse_args()
    args.preview_mode = True
    args.raw_path = None
    args.filename = None
    pipeline_context = PipelineContext(args)

    logs.start_logging(log_level=pipeline_context.log_level)

    try:
        dbs.populate_telescope_tables(db_address=pipeline_context.db_address)
    except Exception as e:
        logger.error('Could not connect to the configdb.')
        logger.error(e)

    logger.info('Starting pipeline preview mode listener')

    for i in range(args.n_processes):
        p = multiprocessing.Process(target=run_indiviudal_listener, args=(args.broker_url,
                                                                          args.queue_name,
                                                                          PipelineContext(args)))
        p.start()

    logs.stop_logging()


def run_indiviudal_listener(broker_url, queue_name, pipeline_context):

    fits_exchange = Exchange('fits_files', type='fanout')
    listener = PreviewModeListener(broker_url, pipeline_context)

    with Connection(listener.broker_url, heartbeat=5) as connection:
        listener.connection = connection
        listener.queue = Queue(queue_name, fits_exchange)
        try:
            listener.run()
        except KeyboardInterrupt:
            logger.info('Shutting down preview pipeline listener.')


class PreviewModeListener(ConsumerMixin):
    def __init__(self, broker_url, pipeline_context):
        self.broker_url = broker_url
        self.pipeline_context = pipeline_context

    def on_connection_error(self, exc, interval):
        logger.error("{0}. Retrying connection in {1} seconds...".format(exc, interval))

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.queue], callbacks=[self.on_message])]

    def on_message(self, body, message):
        path = body.get('path')
        if 'e00.fits' in path or 's00.fits' in path:
            tasks.reduce_preview_image.delay(path, self.pipeline_context)
        message.ack()  # acknowledge to the sender we got this message (it can be popped)
