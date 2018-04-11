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
import traceback
import sys

from kombu import Connection, Queue, Exchange
from kombu.mixins import ConsumerMixin

import banzai.images
from banzai import bias, dark, flats, trim, photometry, astrometry, qc
from banzai import dbs
from banzai import logs
from banzai import crosstalk, gain, mosaic, bpm
from banzai.qc import pointing
from banzai.utils import image_utils, date_utils

logger = logs.get_logger(__name__)

ordered_stages = [qc.HeaderSanity,
                  qc.ThousandsTest,
                  qc.SaturationTest,
                  qc.PatternNoiseDetector,
                  bias.OverscanSubtractor,
                  crosstalk.CrosstalkCorrector,
                  gain.GainNormalizer,
                  mosaic.MosaicCreator,
                  bpm.BPMUpdater,
                  trim.Trimmer,
                  bias.BiasSubtractor,
                  dark.DarkSubtractor,
                  flats.FlatDivider,
                  photometry.SourceDetector,
                  astrometry.WCSSolver,
                  pointing.PointingTest]


def get_stages_todo(last_stage=None, extra_stages=None):
    """

    Parameters
    ----------
    last_stage: banzai.stages.Stage
                Last stage to do
    extra_stages: Stages to do after the last stage

    Returns
    -------
    stages_todo: list of banzai.stages.Stage
                 The stages that need to be done

    Notes
    -----
    Extra stages can be other stages that are not in the ordered_stages list.
    """
    if extra_stages is None:
        extra_stages = []

    if last_stage is None:
        last_index = None
    else:
        last_index = ordered_stages.index(last_stage) + 1

    stages_todo = ordered_stages[:last_index] + extra_stages
    return stages_todo


class PipelineContext(object):
    def __init__(self, args):
        self.processed_path = args.processed_path
        self.raw_path = args.raw_path
        self.post_to_archive = args.post_to_archive
        self.fpack = args.fpack
        self.rlevel = args.rlevel
        self.db_address = args.db_address
        self.log_level = args.log_level
        self.preview_mode = args.preview_mode
        self.filename = args.filename
        self.max_preview_tries = args.max_preview_tries


def run_end_of_night_from_console(scripts_to_run):
    pipeline_context = parse_end_of_night_command_line_arguments()
    logs.start_logging(log_level=pipeline_context.log_level)
    for script in scripts_to_run:
        script(pipeline_context)
    logs.stop_logging()


def make_master_bias(pipeline_context):
    stages_to_do = get_stages_todo(trim.Trimmer, extra_stages=[bias.BiasMaker])
    run(stages_to_do, pipeline_context, image_types=['BIAS'], calibration_maker=True,
        log_message='Making Master BIAS')


def make_master_bias_console():
    run_end_of_night_from_console([make_master_bias])


def make_master_dark(pipeline_context):
    stages_to_do = get_stages_todo(bias.BiasSubtractor, extra_stages=[dark.DarkMaker])
    run(stages_to_do, pipeline_context, image_types=['DARK'], calibration_maker=True,
        log_message='Making Master Dark')


def make_master_dark_console():
    run_end_of_night_from_console([make_master_dark])


def make_master_flat(pipeline_context):
    stages_to_do = get_stages_todo(dark.DarkSubtractor, extra_stages=[flats.FlatMaker])
    run(stages_to_do, pipeline_context, image_types=['SKYFLAT'], calibration_maker=True,
        log_message='Making Master Flat')


def make_master_flat_console():
    run_end_of_night_from_console([make_master_flat])


def reduce_science_frames(pipeline_context):
    stages_to_do = get_stages_todo()
    reduce_frames_one_by_one(stages_to_do, pipeline_context)


def reduce_experimental_frames(pipeline_context):
    stages_to_do = get_stages_todo()
    reduce_frames_one_by_one(stages_to_do, pipeline_context, image_types=['EXPERIMENTAL'])


def reduce_experimental_frames_console():
    run_end_of_night_from_console([reduce_experimental_frames])


def reduce_frames_one_by_one(stages_to_do, pipeline_context, image_types=['EXPOSE', 'STANDARD']):
    image_list = image_utils.make_image_list(pipeline_context)
    original_filename = pipeline_context.filename
    for image in image_list:
        pipeline_context.filename = os.path.basename(image)
        try:
            run(stages_to_do, pipeline_context, image_types=image_types)
        except Exception as e:
            logger.error('{0}'.format(e), extra={'tags': {'filename': pipeline_context.filename,
                                                          'filepath': pipeline_context.raw_path}})
    pipeline_context.filename = original_filename


def reduce_science_frames_console():
    run_end_of_night_from_console([reduce_science_frames])


def reduce_trailed_frames(pipeline_context):
    stages_to_do = get_stages_todo()
    reduce_frames_one_by_one(stages_to_do, pipeline_context, image_types=['TRAILED'])


def reduce_trailed_frames_console():
    run_end_of_night_from_console([reduce_trailed_frames])


def preprocess_sinistro_frames(pipeline_context):
    stages_to_do = get_stages_todo(mosaic.MosaicCreator)
    reduce_frames_one_by_one(stages_to_do, pipeline_context, image_types=['EXPOSE', 'STANDARD',
                                                                          'BIAS', 'DARK', 'SKYFLAT',
                                                                          'TRAILED', 'EXPERIMENTAL'])


def preprocess_sinistro_frames_console():
    run_end_of_night_from_console([preprocess_sinistro_frames])


def create_master_calibrations():
    run_end_of_night_from_console([make_master_bias, make_master_dark, make_master_flat])


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

    args.preview_mode = False
    args.raw_path = None
    args.filename = None
    args.max_preview_tries = 5

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
            pipeline_context.raw_path = os.path.join(args.rawpath_root, args.site, telescope.instrument,
                                                     args.dayobs, 'raw')
            try:
                # Run the reductions on the given dayobs
                make_master_bias(pipeline_context)
                make_master_dark(pipeline_context)
                make_master_flat(pipeline_context)
                reduce_science_frames(pipeline_context)
            except Exception as e:
                logger.error(e)
    logs.stop_logging()


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

    args.preview_mode = False
    args.max_preview_tries = 5

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
                        default='amqp://guest:guest@rabbitmq.lco.gtn:5672/',
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

    with Connection(listener.broker_url) as connection:
        listener.connection = connection.clone()
        listener.queue = Queue(queue_name, fits_exchange)
        try:
            listener.run()
        except listener.connection.connection_errors:
            listener.connection = connection.clone()
            listener.ensure_connection(max_retries=10)
        except KeyboardInterrupt:
            logger.info('Shutting down preview pipeline listener.')


class PreviewModeListener(ConsumerMixin):
    def __init__(self, broker_url, pipeline_context):
        self.broker_url = broker_url
        self.pipeline_context = pipeline_context

    def on_connection_error(self, exc, interval):
        logger.error("{0}. Retrying connection in {1} seconds...".format(exc, interval))
        self.connection = self.connection.clone()
        self.connection.ensure_connection(max_retries=10)

    def get_consumers(self, Consumer, channel):
        consumer = Consumer(queues=[self.queue], callbacks=[self.on_message])
        # Only fetch one thing off the queue at a time
        consumer.qos(prefetch_count=1)
        return [consumer]

    def on_message(self, body, message):
        path = body.get('path')
        message.ack()  # acknowledge to the sender we got this message (it can be popped)

        if 'e00.fits' in path or 's00.fits' in path:
            try:
                if dbs.need_to_make_preview(path, db_address=self.pipeline_context.db_address,
                                            max_tries=self.pipeline_context.max_preview_tries):
                    stages_to_do = get_stages_todo()

                    logging_tags = {'tags': {'filename': os.path.basename(path)}}
                    logger.info('Running preview reduction on {}'.format(path), extra=logging_tags)
                    self.pipeline_context.filename = os.path.basename(path)
                    self.pipeline_context.raw_path = os.path.dirname(path)

                    # Increment the number of tries for this file
                    dbs.increment_preview_try_number(path, db_address=self.pipeline_context.db_address)

                    output_files = run(stages_to_do, self.pipeline_context,
                                       image_types=['EXPOSE', 'STANDARD'])
                    if len(output_files) > 0:
                        dbs.set_preview_file_as_processed(path, db_address=self.pipeline_context.db_address)
                    else:
                        logging_tags = {'tags': {'filename': os.path.basename(path)}}
                        logger.error("Could not produce preview image. {0}".format(path),
                                     extra=logging_tags)
            except Exception as e:
                logging_tags = {'tags': {'filename': os.path.basename(path)}}
                exc_type, exc_value, exc_tb = sys.exc_info()
                tb_msg = traceback.format_exception(exc_type, exc_value, exc_tb)

                logger.error("Exception producing preview frame. {0}. {1}".format(path, tb_msg),
                             extra=logging_tags)
