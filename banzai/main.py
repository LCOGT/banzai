""" main.py: Main driver script for banzai.

    Console entry points to run the BANZAI pipeline.

Author
    Curtis McCully (cmccully@lcogt.net)

October 2015
"""
import argparse
import os.path
import logging
import traceback

from kombu import Exchange, Connection, Queue
from kombu.mixins import ConsumerMixin

from types import ModuleType

from banzai.lco import LCOFrameFactory
from banzai import settings, dbs, logs, calibrations
from banzai.context import Context
from banzai.utils import date_utils, stage_utils, import_utils, image_utils, fits_utils, file_utils
from banzai.celery import process_image, app, schedule_calibration_stacking
from banzai.data import DataProduct
from celery.schedules import crontab
import celery
import celery.bin.beat
import requests

logger = logs.get_logger()


class RealtimeModeListener(ConsumerMixin):
    def __init__(self, runtime_context):
        self.runtime_context = runtime_context
        self.broker_url = runtime_context.broker_url

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
        logger.info('Received message', extra_tags={'filename': body['filename']})
        try:
            instrument = LCOFrameFactory.get_instrument_from_header(body, self.runtime_context.db_address)
        except Exception:
            logger.error(f'Could not get instrument from header. {traceback.format_exc()}', extra_tags={'filename': body['filename']})
            message.ack()
            return
        if instrument is None or instrument.nx is None:
            queue_name = self.runtime_context.CELERY_TASK_QUEUE_NAME
        elif instrument.nx * instrument.ny > self.runtime_context.LARGE_WORKER_THRESHOLD:
            queue_name = self.runtime_context.LARGE_WORKER_QUEUE
        else:
            queue_name = self.runtime_context.CELERY_TASK_QUEUE_NAME
        process_image.apply_async(args=(body, vars(self.runtime_context)),
                                  queue=queue_name)
        message.ack()  # acknowledge to the sender we got this message (it can be popped)


def add_settings_to_context(args, settings):
    # Get all of the settings that are not builtins and store them in the context object
    for setting in dir(settings):
        if '__' != setting[:2] and not isinstance(getattr(settings, setting), ModuleType):
            setattr(args, setting, getattr(settings, setting))


def parse_args(settings, extra_console_arguments=None, parser_description='Process LCO data.',
               parse_system_args=True):
    """
    Create a context object from the given arguments
    :param settings: settings object/module that has defaults for the context object
    :param extra_console_arguments: Non default arguments to add to the command line
    :param parser_description: Help str printed to the console
    :param parse_system_args: Actually parse command line parameters. Setting this False is
        useful to create a context object with defaults to be used for testing.
    :return:
    """
    parser = argparse.ArgumentParser(description=parser_description)

    parser.add_argument("--processed-path", default='/archive/engineering',
                        help='Top level directory where the processed data will be stored')
    parser.add_argument("--log-level", default='info', choices=['debug', 'info', 'warning',
                                                                'critical', 'fatal', 'error'])
    parser.add_argument('--post-to-archive', dest='post_to_archive', action='store_true', default=False)
    parser.add_argument('--no-file-cache', dest='no_file_cache', action='store_true', default=False,
                        help='Turn off saving files to disk')
    parser.add_argument('--post-to-opensearch', dest='post_to_opensearch', action='store_true',
                        default=False)
    parser.add_argument('--fpack', dest='fpack', action='store_true', default=False,
                        help='Fpack the output files?')
    parser.add_argument('--override-missing-calibrations', dest='override_missing', action='store_true', default=False,
                        help='Continue processing a file even if a master calibration does not exist?')
    parser.add_argument('--rlevel', dest='reduction_level', default=91, type=int, help='Reduction level')
    parser.add_argument('--db-address', dest='db_address',
                        default='mysql://cmccully:password@localhost/test',
                        help='Database address: Should be in SQLAlchemy form')
    parser.add_argument('--opensearch-url', dest='opensearch_url',
                        default='https://opensearch.lco.global/')
    parser.add_argument('--os-index', dest='opensearch_qc_index', default='banzai_qc',
                        help='OpenSearch index to use for QC results')
    parser.add_argument('--no-bpm', dest='no_bpm', default=False, action='store_true',
                        help='Do not use a bad pixel mask to reduce data (BPM contains all zeros)')
    parser.add_argument('--use-only-older-calibrations', dest='use_only_older_calibrations', default=False,
                        action='store_true',
                        help='Only use calibrations that were created before the start of the block')
    parser.add_argument('--preview-mode', dest='preview_mode', default=False, action='store_true',
                        help='Save the reductions to the preview directory')
    parser.add_argument('--max-tries', dest='max_tries', default=5,
                        help='Maximum number of times to try to process a frame')
    parser.add_argument('--broker-url', dest='broker_url',
                        help='URL for the FITS broker service.')
    parser.add_argument('--delay-to-block-end', dest='delay_to_block_end', default=False, action='store_true',
                        help='Delay real-time processing until after the block has ended')

    if extra_console_arguments is None:
        extra_console_arguments = []
    for argument in extra_console_arguments:
        parser.add_argument(*argument['args'], **argument['kwargs'])

    if parse_system_args:
        args = parser.parse_args()
    else:
        args = parser.parse_args([])
    logs.set_log_level(args.log_level)

    add_settings_to_context(args, settings)
    return Context(args)


def reduce_single_frame():
    extra_console_arguments = [{'args': ['--filepath'],
                                'kwargs': {'dest': 'path', 'help': 'Full path to the file to process'}}]
    runtime_context = parse_args(settings, extra_console_arguments=extra_console_arguments)
    # Short circuit
    if not image_utils.image_can_be_processed(fits_utils.get_primary_header(runtime_context.path), runtime_context):
        logger.error('Image cannot be processed. Check to make sure the instrument '
                     'is in the database and that the OBSTYPE is recognized by BANZAI',
                     extra_tags={'filename': runtime_context.path})
        return
    try:
        stage_utils.run_pipeline_stages([{'path': runtime_context.path}], runtime_context)
    except Exception:
        logger.error(logs.format_exception(), extra_tags={'filepath': runtime_context.path})


def make_master_calibrations():
    extra_console_arguments = [{'args': ['--site'],
                                'kwargs': {'dest': 'site', 'help': 'Site code (e.g. ogg)', 'required': True}},
                               {'args': ['--camera'],
                                'kwargs': {'dest': 'camera', 'help': 'Camera (e.g. kb95)', 'required': True}},
                               {'args': ['--frame-type'],
                                'kwargs': {'dest': 'frame_type', 'help': 'Type of frames to process',
                                           'choices': ['bias', 'dark', 'skyflat'], 'required': True}},
                               {'args': ['--min-date'],
                                'kwargs': {'dest': 'min_date', 'required': True, 'type': date_utils.validate_date,
                                           'help': 'Earliest observation time of the individual calibration frames. '
                                                   'Must be in the format "YYYY-MM-DDThh:mm:ss".'}},
                               {'args': ['--max-date'],
                                'kwargs': {'dest': 'max_date', 'required': True, 'type': date_utils.validate_date,
                                           'help': 'Latest observation time of the individual calibration frames. '
                                                   'Must be in the format "YYYY-MM-DDThh:mm:ss".'}}]

    runtime_context = parse_args(settings, extra_console_arguments=extra_console_arguments)
    instrument = dbs.query_for_instrument(runtime_context.db_address, runtime_context.site, runtime_context.camera)
    calibrations.make_master_calibrations(instrument,  runtime_context.frame_type.upper(),
                                          runtime_context.min_date, runtime_context.max_date, runtime_context)


def start_stacking_scheduler():
    logger.info('Entered entrypoint to celery beat scheduling')
    runtime_context = parse_args(settings)
    for site, entry in runtime_context.SCHEDULE_STACKING_CRON_ENTRIES.items():
        app.add_periodic_task(crontab(minute=entry['minute'], hour=entry['hour']),
                              schedule_calibration_stacking.s(site=site, runtime_context=vars(runtime_context)),
                              queue=runtime_context.CELERY_TASK_QUEUE_NAME)

    app.Beat(schedule='/tmp/celerybeat-schedule', pidfile='/tmp/celerybeat.pid', working_directory='/tmp').run()
    logger.info('Starting celery beat')

def run_realtime_pipeline():
    extra_console_arguments = [{'args': ['--n-processes'],
                                'kwargs': {'dest': 'n_processes', 'default': 12,
                                           'help': 'Number of listener processes to spawn.', 'type': int}},
                               {'args': ['--queue-name'],
                                'kwargs': {'dest': 'queue_name', 'default': 'banzai_pipeline',
                                           'help': 'Name of the queue to listen to from the fits exchange.'}}]

    runtime_context = parse_args(settings, extra_console_arguments=extra_console_arguments)
    start_listener(runtime_context)


def start_listener(runtime_context):
    # Need to keep the amqp logger level at least as high as INFO,
    # or else it send heartbeat check messages every second
    logging.getLogger('amqp').setLevel(logging.WARNING)
    logger.info('Starting pipeline listener')

    fits_exchange = Exchange(runtime_context.FITS_EXCHANGE, type='fanout')
    listener = RealtimeModeListener(runtime_context)

    with Connection(runtime_context.broker_url) as connection:
        listener.connection = connection.clone()
        listener.queue = Queue(runtime_context.queue_name, fits_exchange)
        try:
            listener.run()
        except listener.connection.connection_errors:
            listener.connection = connection.clone()
            listener.ensure_connection(max_retries=10)
        except KeyboardInterrupt:
            logger.info('Shutting down pipeline listener.')


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


def add_instrument():
    parser = argparse.ArgumentParser(description="Add a new instrument to the database")
    parser.add_argument("--site", help='Site code (e.g. ogg)', required=True)
    parser.add_argument("--camera", help='Camera (e.g. kb95)', required=True)
    parser.add_argument("--name", help='Instrument name (e.g kb05, nres03)', required=True)
    parser.add_argument("--instrument-type", dest='instrument_type',
                        help="Instrument type (e.g. 1m0-SciCam-Sinistro)", required=True)
    parser.add_argument("--nx", help='Number of pixels in x direction', required=True)
    parser.add_argument("--ny", help='Number of pixels in y direction', required=True)
    parser.add_argument('--db-address', dest='db_address', default='sqlite:///test.db',
                        help='Database address: Should be in SQLAlchemy format')
    args = parser.parse_args()
    instrument = {'site': args.site,
                  'camera': args.camera,
                  'type': args.instrument_type,
                  'name': args.name,
                  'nx': args.nx,
                  'ny': args.ny}
    dbs.add_instrument(instrument, args.db_address)


def add_site():
    parser = argparse.ArgumentParser(description="Add a new site to the database")
    parser.add_argument("--site", help='Site code (e.g. ogg)', required=True)
    parser.add_argument("--longitude", help='Longitude (deg)', required=True)
    parser.add_argument("--latitude", help='Latitude (deg)', required=True)
    parser.add_argument("--timezone", help="Time zone relative to UTC", required=True)
    parser.add_argument("--elevation", help="Elevation of site (m)", required=True)
    parser.add_argument('--db-address', dest='db_address', default='sqlite:///test.db',
                        help='Database address: Should be in SQLAlchemy format')
    args = parser.parse_args()
    site = {'code': args.site,
            'longitude': args.longitude,
            'latitude': args.latitude,
            'elevation': args.elevation,
            'timezone': args.timezone}
    dbs.add_site(site, args.db_address)


def mark_frame_as_good():
    mark_frame("good")


def mark_frame_as_bad():
    mark_frame("bad")


def update_db():
    parser = argparse.ArgumentParser(description="Query the configdb to ensure that the instruments table"
                                                 "has the most up-to-date information")

    parser.add_argument("--log-level", default='debug', choices=['debug', 'info', 'warning',
                                                                 'critical', 'fatal', 'error'])
    parser.add_argument('--configdb-address', dest='configdb_address',
                        default='http://configdb.lco.gtn/sites/',
                        help='URL of the configdb with instrument information')
    parser.add_argument('--db-address', dest='db_address',
                        default='mysql://cmccully:password@localhost/test',
                        help='Database address: Should be in SQLAlchemy form')
    args = parser.parse_args()
    logs.set_log_level(args.log_level)

    try:
        dbs.populate_instrument_tables(db_address=args.db_address, configdb_address=args.configdb_address)
    except Exception:
        logger.error('Could not populate instruments table: {error}'.format(error=logs.format_exception()))


def add_super_calibration():
    parser = argparse.ArgumentParser(description="Add a super calibration file to the db.")
    parser.add_argument('filepath', help='Full path to calibration file')
    parser.add_argument("--log-level", default='debug', choices=['debug', 'info', 'warning',
                                                                 'critical', 'fatal', 'error'])
    parser.add_argument('--db-address', dest='db_address',
                        default='mysql://cmccully:password@localhost/test',
                        help='Database address: Should be in SQLAlchemy form')
    parser.add_argument('--skip-ingester', dest='skip_ingester', action='store_true')
    parser.add_argument('--frame-id', dest='frame_id', help="Frame ID for this calibration frame, if sourced from the archive.")
    args = parser.parse_args()
    add_settings_to_context(args, settings)
    logs.set_log_level(args.log_level)
    frame_factory = import_utils.import_attribute(settings.FRAME_FACTORY)()

    try:
        cal_image = frame_factory.open({'path': args.filepath}, args)
    except Exception:
        logger.error(f"Calibration file not able to be opened by BANZAI. Aborting... {logs.format_exception()}",
                     extra_tags={'filename': args.filepath})
        return

    if args.skip_ingester:
        logger.debug("Skipped posting frame to archive. Saving to database.", extra_tags={'frameid': args.frame_id})
        cal_image.frameid = args.frame_id

    # upload calibration file via ingester
    else:
        with open(args.filepath, 'rb') as f:
            logger.debug("Posting calibration file to s3 archive")
            ingester_response = file_utils.post_to_ingester(f, cal_image, args.filepath)
        frame_id = ingester_response['frameid']
        logger.debug("File posted to s3 archive. Saving to database.", extra_tags={'frameid': frame_id})
        cal_image.frameid = frame_id

    cal_image.is_bad = False
    cal_image.is_master = True
    dbs.save_calibration_info(
        cal_image.to_db_record(
            DataProduct(
                None,
                filename=os.path.basename(args.filepath),
                filepath=os.path.dirname(args.filepath)
            )
        ),
        args.db_address
    )


def add_bpms_from_archive():
    parser = argparse.ArgumentParser(description="Add bad pixel mask from a given archive api")
    parser.add_argument('--db-address', dest='db_address',
                        default='mysql://cmccully:password@localhost/test',
                        help='Database address: Should be in SQLAlchemy form')
    args = parser.parse_args()
    add_settings_to_context(args, settings)
    # Query the archive for all bpm files
    url = f'{settings.ARCHIVE_FRAME_URL}/?OBSTYPE=BPM&limit=1000'
    archive_auth_header = settings.ARCHIVE_AUTH_HEADER
    response = requests.get(url, headers=archive_auth_header)
    response.raise_for_status()
    results = response.json()['results']

    # Load each one, saving the calibration info for each
    frame_factory = import_utils.import_attribute(settings.FRAME_FACTORY)()
    for frame in results:
        frame['frameid'] = frame['id']
        try:
            bpm_image = frame_factory.open(frame, args)
            if bpm_image is not None:
                bpm_image.is_master = True
                dbs.save_calibration_info(bpm_image.to_db_record(DataProduct(None, filename=bpm_image.filename,
                                                                             filepath=None)),
                                          args.db_address)
        except Exception:
            logger.error(f"BPM not added to database: {logs.format_exception()}",
                         extra_tags={'filename': frame.get('filename')})


def create_db():
    """
    Create the database structure.

    This only needs to be run once on initialization of the database.
    """
    parser = argparse.ArgumentParser("Create the database.\n\n"
                                     "This only needs to be run once on initialization of the database.")

    parser.add_argument("--log-level", default='debug', choices=['debug', 'info', 'warning',
                                                                 'critical', 'fatal', 'error'])
    parser.add_argument('--db-address', dest='db_address',
                        default='sqlite3:///test.db',
                        help='Database address: Should be in SQLAlchemy form')
    args = parser.parse_args()
    logs.set_log_level(args.log_level)

    dbs.create_db(args.db_address)
