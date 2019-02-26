""" main.py: Main driver script for banzai.

    The main() function is a console entry point.

Author
    Curtis McCully (cmccully@lcogt.net)

October 2015
"""
import argparse
import multiprocessing
import os
import logging
import copy
import sys

from kombu import Exchange, Connection, Queue
from kombu.mixins import ConsumerMixin
from lcogt_logging import LCOGTFormatter

import banzai.context
from banzai import dbs, realtime, logs
from banzai.context import Context
from banzai.utils import image_utils, date_utils, fits_utils
from banzai.images import read_image
from banzai import settings


# Logger set up
logging.captureWarnings(True)
# Set up the root logger
root_logger = logging.getLogger()
root_handler = logging.StreamHandler(sys.stdout)
# Add handler
formatter = LCOGTFormatter()
root_handler.setFormatter(formatter)
root_handler.setLevel(getattr(logging, 'DEBUG'))
root_logger.addHandler(root_handler)


logger = logging.getLogger(__name__)

RAW_PATH_CONSOLE_ARGUMENT = {'args': ["--raw-path"],
                             'kwargs': {'dest': 'raw_path', 'default': '/archive/engineering',
                                        'help': 'Top level directory where the raw data is stored'}}


def get_stages_todo(ordered_stages, last_stage=None, extra_stages=None):
    """

    Parameters
    ----------
    ordered_stages: list of banzai.stages.Stage objects
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


def parse_args(settings, extra_console_arguments=None,
               parser_description='Process LCO data.', **kwargs):
    """Parse arguments, including default command line argument, and set the overall log level"""

    parser = argparse.ArgumentParser(description=parser_description)

    parser.add_argument("--processed-path", default='/archive/engineering',
                        help='Top level directory where the processed data will be stored')
    parser.add_argument("--log-level", default='debug', choices=['debug', 'info', 'warning',
                                                                 'critical', 'fatal', 'error'])
    parser.add_argument('--post-to-archive', dest='post_to_archive', action='store_true',
                        default=False)
    parser.add_argument('--post-to-elasticsearch', dest='post_to_elasticsearch', action='store_true',
                        default=False)
    parser.add_argument('--fpack', dest='fpack', action='store_true', default=False,
                        help='Fpack the output files?')
    parser.add_argument('--rlevel', dest='rlevel', default=91, type=int, help='Reduction level')
    parser.add_argument('--db-address', dest='db_address',
                        default='mysql://cmccully:password@localhost/test',
                        help='Database address: Should be in SQLAlchemy form')
    parser.add_argument('--elasticsearch-url', dest='elasticsearch_url',
                        default='http://elasticsearch.lco.gtn:9200')
    parser.add_argument('--es-index', dest='elasticsearch_qc_index', default='banzai_qc',
                        help='ElasticSearch index to use for QC results')
    parser.add_argument('--es-doc-type', dest='elasticsearch_doc_type', default='qc',
                        help='Elasticsearch document type for QC records')
    parser.add_argument('--no-bpm', dest='no_bpm', default=False, action='store_true',
                        help='Do not use a bad pixel mask to reduce data (BPM contains all zeros)')
    parser.add_argument('--ignore-schedulability', dest='ignore_schedulability',
                        default=False, action='store_true',
                        help='Relax requirement that the instrument be schedulable')

    if extra_console_arguments is None:
        extra_console_arguments = []
    for argument in extra_console_arguments:
        parser.add_argument(*argument['args'], **argument['kwargs'])
    args = parser.parse_args()

    logs.set_log_level(args.log_level)

    # TODO:
    # if not args.ignore_schedulability:
    #     settings.FRAME_SELECTION_CRITERIA += settings.SCHEDULABLE_CRITERIA

    runtime_context = Context(args)

    return runtime_context


def run(image_path, pipeline_context):
    """
    Main driver script for banzai.
    """
    image = read_image(image_path)
    stages_to_do = get_stages_todo(settings.ORDERED_STAGES,
                                   last_stage=settings.LAST_STAGE[image.obstype],
                                   extra_stages=settings.EXTRA_STAGES[image.obstype])
    logger.info("Starting to reduce frame", image=image)
    for stage in stages_to_do:
        stage_to_run = stage(pipeline_context)
        image = stage_to_run.run(image)
    if image is None:
        logger.error('Reduction stopped', extra_tags={'filename': image_path})
        return
    image.write(pipeline_context)
    logger.info("Finished reducing frame", image=image)


def run_master_maker(image_path_list, pipeline_context, frame_type):
    images = [read_image(image_path, pipeline_context) for image_path in image_path_list]
    stage_to_run = settings.CALIBRATION_STACKER_STAGE[frame_type](pipeline_context)
    images = stage_to_run.run(images)
    for image in images:
        image.write(pipeline_context)


def process_directory(pipeline_context, raw_path, image_types=None, log_message=''):
    if len(log_message) > 0:
        logger.info(log_message, extra_tags={'raw_path': raw_path})
    image_path_list = image_utils.make_image_path_list(raw_path)
    if image_types is None:
        image_types = [None]
    images_to_reduce = []
    for image_type in image_types:
        images_to_reduce += image_utils.select_images(image_path_list, pipeline_context, image_type)
    for image_path in images_to_reduce:
        try:
            run(image_path, pipeline_context)
        except Exception:
            logger.error(logs.format_exception(), extra_tags={'filename': image_path})


def process_single_frame(pipeline_context, raw_path, filename, log_message=''):
    if len(log_message) > 0:
        logger.info(log_message, extra_tags={'raw_path': raw_path, 'filename': filename})
    full_path = os.path.join(raw_path, filename)
    # Short circuit
    if not pipeline_context.image_can_be_processed(fits_utils.get_primary_header(full_path)):
        logger.error('Image cannot be processed. Check to make sure the instrument '
                     'is in the database and that the OBSTYPE is recognized by BANZAI',
                     extra_tags={'raw_path': raw_path, 'filename': filename})
        return
    try:
        run(full_path, pipeline_context)
    except Exception:
        logger.error(logs.format_exception(), extra_tags={'filename': filename})


def process_master_maker(pipeline_context, instrument, frame_type, min_date, max_date, use_masters=False):
    extra_tags = {'instrument': instrument.camera, 'obstype': frame_type,
                  'min_date': min_date.strftime(date_utils.TIMESTAMP_FORMAT),
                  'max_date': max_date.strftime(date_utils.TIMESTAMP_FORMAT)}
    logger.info("Making master frames", extra_tags=extra_tags)
    image_path_list = dbs.get_individual_calibration_images(instrument, frame_type, min_date, max_date,
                                                            use_masters=use_masters,
                                                            db_address=pipeline_context.db_address)
    if len(image_path_list) == 0:
        logger.info("No calibration frames found to stack", extra_tags=extra_tags)

    try:
        run_master_maker(image_path_list, pipeline_context, frame_type)
    except Exception:
        logger.error(logs.format_exception())
    logger.info("Finished")


def parse_directory_args(pipeline_context=None, raw_path=None, settings=None, extra_console_arguments=None):
    if extra_console_arguments is None:
        extra_console_arguments = []

    if pipeline_context is None:
        if settings is None:
            logger.error("Cannot create a pipeline context without any settings")
            raise Exception
        if raw_path is None:
            extra_console_arguments += [RAW_PATH_CONSOLE_ARGUMENT]

        pipeline_context = parse_args(settings, extra_console_arguments=extra_console_arguments)

        if raw_path is None:
            raw_path = pipeline_context.raw_path
    return pipeline_context, raw_path


def reduce_directory(pipeline_context=None, raw_path=None, image_types=None):
    # TODO: Remove image_types once reduce_night is not needed
    pipeline_context, raw_path = parse_directory_args(pipeline_context, raw_path, banzai.settings.ImagingSettings())
    process_directory(pipeline_context, raw_path, image_types=image_types,
                      log_message='Reducing all frames in directory')


def reduce_single_frame(pipeline_context=None):
    extra_console_arguments = [{'args': ['--filename'],
                                'kwargs': {'dest': 'filename', 'help': 'Name of file to process'}}]
    pipeline_context, raw_path = parse_directory_args(pipeline_context, None, banzai.settings.ImagingSettings(),
                                                      extra_console_arguments=extra_console_arguments)
    process_single_frame(pipeline_context, raw_path, pipeline_context.filename)


def stack_calibrations(pipeline_context=None, raw_path=None):
    extra_console_arguments = [{'args': ['--site'],
                                'kwargs': {'dest': 'site', 'help': 'Site code (e.g. ogg)', 'required': True}},
                               {'args': ['--camera'],
                                'kwargs': {'dest': 'camera', 'help': 'Camera (e.g. kb95)', 'required': True}},
                               {'args': ['--frame-type'],
                                'kwargs': {'dest': 'frame_type', 'help': 'Type of frames to process',
                                           'choices': ['bias', 'dark', 'skyflat'], 'required': True}},
                               {'args': ['--min-date'],
                                'kwargs': {'dest': 'min_date', 'required': True, 'type': date_utils.valid_date,
                                           'help': 'Earliest observation time of the individual calibration frames. '
                                                   'Must be in the format "YYYY-MM-DDThh:mm:ss".'}},
                               {'args': ['--max-date'],
                                'kwargs': {'dest': 'max_date', 'required': True, 'type': date_utils.valid_date,
                                           'help': 'Latest observation time of the individual calibration frames. '
                                                   'Must be in the format "YYYY-MM-DDThh:mm:ss".'}}]

    pipeline_context, raw_path = parse_directory_args(pipeline_context, raw_path, banzai.settings.ImagingSettings(),
                                                      extra_console_arguments=extra_console_arguments)
    instrument = dbs.query_for_instrument(pipeline_context.db_address, pipeline_context.site, pipeline_context.camera)
    process_master_maker(pipeline_context, instrument,  pipeline_context.frame_type.upper(),
                         pipeline_context.min_date, pipeline_context.max_date)


def run_end_of_night():
    extra_console_arguments = [{'args': ['--site'],
                                'kwargs': {'dest': 'site', 'help': 'Site code (e.g. ogg)'}},
                               {'args': ['--dayobs'],
                                'kwargs': {'dest': 'dayobs', 'default': None,
                                           'help': 'Day-Obs to reduce (e.g. 20160201)'}},
                               {'args': ['--raw-path-root'],
                                'kwargs': {'dest': 'rawpath_root', 'default': '/archive/engineering',
                                           'help': 'Top level directory with raw data.'}}]

    pipeline_context = parse_args(banzai.settings.ImagingSettings, extra_console_arguments=extra_console_arguments,
                                  parser_description='Reduce all the data from a site at the end of a night.')

    # Ping the configdb to get instruments
    try:
        dbs.populate_instrument_tables(db_address=pipeline_context.db_address)
    except Exception:
        logger.error('Could not connect to the configdb: {error}'.format(error=logs.format_exception()))

    try:
        timezone = dbs.get_timezone(pipeline_context.site, db_address=pipeline_context.db_address)
    except dbs.SiteMissingException:
        msg = "Site {site} not found in database {db}, exiting."
        logger.error(msg.format(site=pipeline_context.site, db=pipeline_context.db_address),
                     extra_tags={'site': pipeline_context.site})
        return

    # If no dayobs is given, calculate it.
    if pipeline_context.dayobs is None:
        dayobs = date_utils.get_dayobs(timezone=timezone)
    else:
        dayobs = pipeline_context.dayobs

    instruments = dbs.get_instruments_at_site(pipeline_context.site,
                                              db_address=pipeline_context.db_address,
                                              ignore_schedulability=pipeline_context.ignore_schedulability)
    instruments = [instrument for instrument in instruments
                   if banzai.context.instrument_passes_criteria(instrument, pipeline_context.FRAME_SELECTION_CRITERIA)]
    # For each instrument at the given site
    for instrument in instruments:
        raw_path = os.path.join(pipeline_context.rawpath_root, pipeline_context.site,
                                instrument.camera, dayobs, 'raw')
        try:
            reduce_directory(pipeline_context, raw_path, image_types=['EXPOSE', 'STANDARD'])
        except Exception:
            logger.error(logs.format_exception())


def run_realtime_pipeline():
    extra_console_arguments = [{'args': ['--n-processes'],
                                'kwargs': {'dest': 'n_processes', 'default': 12,
                                           'help': 'Number of listener processes to spawn.', 'type': int}},
                               {'args': ['--broker-url'],
                                'kwargs': {'dest': 'broker_url', 'default': 'amqp://guest:guest@rabbitmq.lco.gtn:5672/',
                                           'help': 'URL for the broker service.'}},
                               {'args': ['--queue-name'],
                                'kwargs': {'dest': 'queue_name', 'default': 'banzai_pipeline',
                                           'help': 'Name of the queue to listen to from the fits exchange.'}},
                               {'args': ['--preview-mode'],
                                'kwargs': {'dest': 'preview_mode', 'default': False,
                                           'help': 'Save the real-time reductions to the preview directory'}}]

    pipeline_context = parse_args(banzai.settings.ImagingSettings,
                                  parser_description='Reduce LCO imaging data in real time.',
                                  extra_console_arguments=extra_console_arguments, realtime_reduction=True)

    # Need to keep the amqp logger level at least as high as INFO,
    # or else it send heartbeat check messages every second
    logging.getLogger('amqp').setLevel(max(logger.level, getattr(logging, 'INFO')))

    try:
        dbs.populate_instrument_tables(db_address=pipeline_context.db_address)
    except Exception:
        logger.error('Could not connect to the configdb: {error}'.format(error=logs.format_exception()))

    logger.info('Starting pipeline listener')

    for i in range(pipeline_context.n_processes):
        p = multiprocessing.Process(target=run_individual_listener, args=(pipeline_context.broker_url,
                                                                          pipeline_context.queue_name,
                                                                          copy.deepcopy(pipeline_context)))
        p.start()


def run_individual_listener(broker_url, queue_name, pipeline_context):

    fits_exchange = Exchange('fits_files', type='fanout')
    listener = RealtimeModeListener(broker_url, pipeline_context)

    with Connection(listener.broker_url) as connection:
        listener.connection = connection.clone()
        listener.queue = Queue(queue_name, fits_exchange)
        try:
            listener.run()
        except listener.connection.connection_errors:
            listener.connection = connection.clone()
            listener.ensure_connection(max_retries=10)
        except KeyboardInterrupt:
            logger.info('Shutting down pipeline listener.')


class RealtimeModeListener(ConsumerMixin):
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
        try:
            if realtime.need_to_process_image(path, self.pipeline_context,
                                              db_address=self.pipeline_context.db_address,
                                              max_tries=self.pipeline_context.max_tries):

                logger.info('Reducing frame', extra_tags={'filename': os.path.basename(path)})

                # Increment the number of tries for this file
                realtime.increment_try_number(path, db_address=self.pipeline_context.db_address)

                run(path, self.pipeline_context)
                realtime.set_file_as_processed(path, db_address=self.pipeline_context.db_address)

        except Exception:
            logger.error("Exception processing frame: {error}".format(error=logs.format_exception()),
                         extra_tags={'filename': os.path.basename(path)})


def mark_frame(mark_as):
    parser = argparse.ArgumentParser(description="Set the is_bad flag to mark the frame as {mark_as}"
                                                 "for a calibration frame in the database ".format(mark_as=mark_as))
    parser.add_argument('--filename', dest='filename', required=True,
                        help='Name of calibration file to be marked')
    parser.add_argument('--db-address', dest='db_address',
                        default='mysql://cmccully:password@localhost/test',
                        help='Database address: Should be in SQLAlchemy form')
    parser.add_argument("--log-level", default='debug', choices=['debug', 'info', 'warning',
                                                                 'critical', 'fatal', 'error'])

    args = parser.parse_args()
    logs.set_log_level(args.log_level)

    logger.info("Marking the frame {filename} as {mark_as}".format(filename=args.filename, mark_as=mark_as))
    dbs.mark_frame(args.filename, mark_as, db_address=args.db_address)
    logger.info("Finished")


def mark_frame_as_good():
    mark_frame("good")


def mark_frame_as_bad():
    mark_frame("bad")
