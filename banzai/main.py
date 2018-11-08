""" main.py: Main driver script for banzai.

    The main() function is a console entry point.

Author
    Curtis McCully (cmccully@lcogt.net)

October 2015
"""
import argparse
import multiprocessing
import os
import sys
import traceback
import logging
import copy

from kombu import Exchange, Connection, Queue
from kombu.mixins import ConsumerMixin

from banzai import settings
from banzai import dbs, preview, logs
from banzai.context import PipelineContext
from banzai.utils import image_utils, date_utils
import banzai.images

logger = logging.getLogger(__name__)

RAW_PATH_CONSOLE_ARGUMENT = {'args': ["--raw-path"],
                             'kwargs': {'dest': 'raw_path', 'default': '/archive/engineering',
                                        'help': 'Top level directory where the raw data is stored'}}


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
        last_index = settings.ORDERED_STAGES.index(last_stage) + 1

    stages_todo = settings.ORDERED_STAGES[:last_index] + extra_stages
    return stages_todo


def parse_args(selection_criteria, extra_console_arguments=None,
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
                        help='Relax requirement that the telescope be schedulable')

    if extra_console_arguments is None:
        extra_console_arguments = []

    for argument in extra_console_arguments:
        parser.add_argument(*argument['args'], **argument['kwargs'])

    args = parser.parse_args()

    logs.set_log_level(args.log_level)

    if not args.ignore_schedulability:
        selection_criteria += settings.SCHEDULABLE_CRITERIA

    pipeline_context = PipelineContext(args, selection_criteria, **kwargs)

    return pipeline_context


def run(stages_to_do, image_path, pipeline_context):
    """
    Main driver script for banzai.
    """
    image = banzai.images.read_image(image_path, pipeline_context)

    for stage in stages_to_do:
        stage_to_run = stage(pipeline_context)
        image = stage_to_run.run(image)

    image_utils.save_image(pipeline_context, image)
    return image


def run_calibration_maker(stage_to_do, image_list, pipeline_context):
    stage_to_run = stage_to_do(pipeline_context)
    image_list = stage_to_run.run(image_list)
    for image in image_list:
        image_utils.save_image(pipeline_context, image, master_calibration=True)
    return image_list


def process_directory(pipeline_context, raw_path, image_types=None, last_stage=None, extra_stages=None, log_message='',
                      calibration_maker_stage=None):
    if len(log_message) > 0:
        logger.info(log_message, extra_tags={'raw_path': raw_path})
    stages_to_do = get_stages_todo(last_stage, extra_stages=extra_stages)
    image_path_list = image_utils.make_image_path_list(raw_path)
    image_path_list = image_utils.select_images(image_path_list, image_types,
                                                pipeline_context.allowed_instrument_criteria,
                                                db_address=pipeline_context.db_address)
    try:
        image_list = []
        for image_path in image_path_list:
            image_list.append(run(stages_to_do, image_path, pipeline_context))
        if calibration_maker_stage is not None:
            run_calibration_maker(calibration_maker_stage, image_list, pipeline_context)
    except Exception as e:
        logger.error(e, extra_tags={'raw_path': raw_path})


def process_single_frame(pipeline_context, raw_path, filename, last_stage=None, extra_stages=None, log_message=''):
    if len(log_message) > 0:
        logger.info(log_message, extra_tags={'raw_path': raw_path, 'filename': filename})
    stages_to_do = get_stages_todo(last_stage, extra_stages=extra_stages)
    try:
        run(stages_to_do, [os.path.join(raw_path, filename)], pipeline_context, calibration_maker=False)
    except Exception as e:
        logger.error(e, extra_tags={'filename': filename})


def parse_directory_args(pipeline_context, raw_path, selection_criteria, extra_console_arguments=None):
    if extra_console_arguments is None:
        extra_console_arguments = []

    if pipeline_context is None:
        if raw_path is None:
            extra_console_arguments += [RAW_PATH_CONSOLE_ARGUMENT]

        pipeline_context = parse_args(selection_criteria, extra_console_arguments=extra_console_arguments)

        if raw_path is None:
            raw_path = pipeline_context.raw_path
    return pipeline_context, raw_path


def make_master_bias(pipeline_context=None, raw_path=None):
    pipeline_context, raw_path = parse_directory_args(pipeline_context, raw_path, settings.IMAGING_CRITERIA)
    process_directory(pipeline_context, raw_path, settings.BIAS_IMAGE_TYPES, last_stage=settings.BIAS_LAST_STAGE,
                      extra_stages=settings.BIAS_EXTRA_STAGES, calibration_maker_stage=settings.BIAS_MAKER_STAGE,
                      log_message='Making Master Bias')


def make_master_dark(pipeline_context=None, raw_path=None):
    pipeline_context, raw_path = parse_directory_args(pipeline_context, raw_path, settings.IMAGING_CRITERIA)
    process_directory(pipeline_context, raw_path, settings.DARK_IMAGE_TYPES, last_stage=settings.DARK_LAST_STAGE,
                      extra_stages=settings.DARK_EXTRA_STAGES, calibration_maker_stage=settings.DARK_MAKER_STAGE,
                      log_message='Making Master Dark')


def make_master_flat(pipeline_context=None, raw_path=None):
    pipeline_context, raw_path = parse_directory_args(pipeline_context, raw_path, settings.IMAGING_CRITERIA)
    process_directory(pipeline_context, raw_path, settings.FLAT_IMAGE_TYPES, last_stage=settings.FLAT_LAST_STAGE,
                      extra_stages=settings.FLAT_EXTRA_STAGES, calibration_maker_stage=settings.FLAT_MAKER_STAGE,
                      log_message='Making Master Flat')


def reduce_science_frames(pipeline_context=None, raw_path=None):
    pipeline_context, raw_path = parse_directory_args(pipeline_context, raw_path, settings.IMAGING_CRITERIA)
    process_directory(pipeline_context, raw_path, settings.SCIENCE_IMAGE_TYPES)


def reduce_single_science_frame():
    extra_console_arguments = [{'args': ['--filename'],
                                'kwargs': {'dest': 'filename', 'help': 'Name of file to process'}}]
    pipeline_context, raw_path = parse_directory_args(None, None, settings.IMAGING_CRITERIA,
                                                      extra_console_arguments=extra_console_arguments)
    process_single_frame(pipeline_context, raw_path, pipeline_context.filename)


def reduce_experimental_frames(pipeline_context=None, raw_path=None):
    pipeline_context, raw_path = parse_directory_args(pipeline_context, raw_path, settings.IMAGING_CRITERIA)
    process_directory(pipeline_context, raw_path, settings.EXPERIMENTAL_IMAGE_TYPES)


def reduce_trailed_frames(pipeline_context=None, raw_path=None):
    pipeline_context, raw_path = parse_directory_args(pipeline_context, raw_path, settings.IMAGING_CRITERIA)
    process_directory(pipeline_context, raw_path, settings.TRAILED_IMAGE_TYPES)


def preprocess_sinistro_frames(pipeline_context=None, raw_path=None):
    pipeline_context, raw_path = parse_directory_args(pipeline_context, raw_path, settings.IMAGING_CRITERIA)
    process_directory(pipeline_context, raw_path, settings.SINISTRO_IMAGE_TYPES,
                      last_stage=settings.SINISTRO_LAST_STAGE)


def reduce_night():
    extra_console_arguments = [{'args': ['--site'], 'kwargs': {'dest': 'site', 'help': 'Site code (e.g. ogg)'}},
                               {'args': ['--dayobs'], 'kwargs': {'dest': 'dayobs', 'default': None,
                                                               'help': 'Day-Obs to reduce (e.g. 20160201)'}},
                               {'args': ['--raw-path-root'],
                                'kwargs': {'dest': 'rawpath_root', 'default': '/archive/engineering',
                                           'help': 'Top level directory with raw data.'}}]

    pipeline_context = parse_args(settings.IMAGING_CRITERIA, extra_console_arguments=extra_console_arguments,
                                  parser_description='Reduce all the data from a site at the end of a night.')

    # Ping the configdb to get telescopes
    try:
        dbs.populate_telescope_tables(db_address=pipeline_context.db_address)
    except Exception as e:
        logger.error('Could not connect to the configdb.')
        logger.error(e)

    try:
        timezone = dbs.get_timezone(pipeline_context.site, db_address=pipeline_context.db_address)
    except dbs.SiteMissingException:
        logger.error("Site {site} not found in database {db}, exiting.".format(site=pipeline_context.site,
                                                                               db=pipeline_context.db_address),
                     extra_tags={'site': pipeline_context.site})
        return

    telescopes = dbs.get_telescopes_at_site(pipeline_context.site,
                                            db_address=pipeline_context.db_address,
                                            ignore_schedulability=pipeline_context.ignore_schedulability)

    # If no dayobs is given, calculate it.
    if pipeline_context.dayobs is None:
        dayobs = date_utils.get_dayobs(timezone=timezone)
    else:
        dayobs = pipeline_context.dayobs

    # For each telescope at the given site
    for telescope in telescopes:
        raw_path = os.path.join(pipeline_context.rawpath_root, pipeline_context.site,
                                telescope.instrument, dayobs, 'raw')

    # Run the reductions on the given dayobs
    try:
        make_master_bias(pipeline_context=pipeline_context, raw_path=raw_path)
    except Exception as e:
        logger.error(e)
    try:
        make_master_dark(pipeline_context=pipeline_context, raw_path=raw_path)
    except Exception as e:
        logger.error(e)
    try:
        make_master_flat(pipeline_context=pipeline_context, raw_path=raw_path)
    except Exception as e:
        logger.error(e)
    try:
        reduce_science_frames(pipeline_context=pipeline_context, raw_path=raw_path)
    except Exception as e:
        logger.error(e)


def get_preview_stages_todo(image_suffix):
    if image_suffix in settings.BIAS_SUFFIXES:
        stages = get_stages_todo(last_stage=settings.BIAS_LAST_STAGE,
                                 extra_stages=settings.BIAS_EXTRA_STAGES_PREVIEW)
    elif image_suffix in settings.DARK_SUFFIXES:
        stages = get_stages_todo(last_stage=settings.DARK_LAST_STAGE,
                                 extra_stages=settings.DARK_EXTRA_STAGES_PREVIEW)
    elif image_suffix in settings.FLAT_SUFFIXES:
        stages = get_stages_todo(last_stage=settings.FLAT_LAST_STAGE,
                                 extra_stages=settings.FLAT_EXTRA_STAGES_PREVIEW)
    else:
        stages = get_stages_todo()
    return stages


def run_preview_pipeline():
    extra_console_arguments = [{'args': ['--n-processes'],
                                'kwargs': {'dest': 'n_processes', 'default': 12,
                                           'help': 'Number of listener processes to spawn.', 'type': int}},
                               {'args': ['--broker-url'],
                                'kwargs': {'dest': 'broker_url', 'default': 'amqp://guest:guest@rabbitmq.lco.gtn:5672/',
                                           'help': 'URL for the broker service.'}},
                               {'args': ['--queue-name'],
                                'kwargs': {'dest': 'queue_name', 'default': 'preview_pipeline',
                                           'help': 'Name of the queue to listen to from the fits exchange.'}}]

    pipeline_context = parse_args(settings.IMAGING_CRITERIA,
                                  parser_description='Reduce LCO imaging data in real time.',
                                  extra_console_arguments=extra_console_arguments, preview_mode=True)

    # Need to keep the amqp logger level at least as high as INFO,
    # or else it send heartbeat check messages every second
    logging.getLogger('amqp').setLevel(max(logger.level, getattr(logging, 'INFO')))

    try:
        dbs.populate_telescope_tables(db_address=pipeline_context.db_address)
    except Exception as e:
        logger.error('Could not connect to the configdb.')
        logger.error(e)

    logger.info('Starting pipeline preview mode listener')

    for i in range(pipeline_context.n_processes):
        p = multiprocessing.Process(target=run_individual_listener, args=(pipeline_context.broker_url,
                                                                          pipeline_context.queue_name,
                                                                          copy.deepcopy(pipeline_context)))
        p.start()


def run_individual_listener(broker_url, queue_name, pipeline_context):

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

        is_eligible_for_preview = False
        for suffix in settings.PREVIEW_ELIGIBLE_SUFFIXES:
            if suffix in path:
                is_eligible_for_preview = True
                image_suffix = suffix

        if is_eligible_for_preview:
            try:
                if preview.need_to_make_preview(path, self.pipeline_context.allowed_instrument_criteria,
                                                db_address=self.pipeline_context.db_address,
                                                max_tries=self.pipeline_context.max_tries):
                    stages_to_do = get_preview_stages_todo(image_suffix)

                    logger.info('Running preview reduction', extra_tags={'filename': os.path.basename(path)})

                    # Increment the number of tries for this file
                    preview.increment_preview_try_number(path, db_address=self.pipeline_context.db_address)

                    run(stages_to_do, [path], self.pipeline_context)
                    preview.set_preview_file_as_processed(path, db_address=self.pipeline_context.db_address)

            except Exception:
                exc_type, exc_value, exc_tb = sys.exc_info()
                tb_msg = traceback.format_exception(exc_type, exc_value, exc_tb)

                logger.error("Exception producing preview frame. {error}".format(error=tb_msg),
                             extra_tags={'filename': os.path.basename(path)})
