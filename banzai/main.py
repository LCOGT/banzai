""" main.py: Main driver script for banzai.

    The main() function is a console entry point.

Author
    Curtis McCully (cmccully@lcogt.net)

October 2015
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import argparse

import banzai.images
from banzai.utils import image_utils, date_utils
from banzai import munge, crosstalk, gain, mosaic
from banzai import bias, dark, flats, trim, photometry, astrometry, headers
from banzai import logs
from banzai import dbs
import os
import multiprocessing
from kombu.mixins import ConsumerMixin
from kombu import Connection, Queue, Exchange

logger = logs.get_logger(__name__)


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


def make_master_bias(pipeline_context=None):
    if pipeline_context is None:
        pipeline_context = parse_end_of_night_command_line_arguments()
    logs.start_logging(log_level=pipeline_context.log_level)
    stages_to_do = [munge.DataMunger, crosstalk.CrosstalkCorrector, bias.OverscanSubtractor,
                    gain.GainNormalizer, mosaic.MosaicCreator, trim.Trimmer, bias.BiasMaker,
                    headers.HeaderUpdater]
    run(stages_to_do, pipeline_context, image_types=['BIAS'], calibration_maker=True,
        log_message='Making Master BIAS')
    logs.stop_logging()


def make_master_dark(pipeline_context=None):
    if pipeline_context is None:
        pipeline_context = parse_end_of_night_command_line_arguments()

    logs.start_logging(log_level=pipeline_context.log_level)
    stages_to_do = [munge.DataMunger, crosstalk.CrosstalkCorrector, bias.OverscanSubtractor,
                    gain.GainNormalizer, mosaic.MosaicCreator, trim.Trimmer,
                    bias.BiasSubtractor, dark.DarkMaker, headers.HeaderUpdater]
    run(stages_to_do, pipeline_context, image_types=['DARK'], calibration_maker=True,
        log_message='Making Master Dark')
    logs.stop_logging()


def make_master_flat(pipeline_context=None):
    if pipeline_context is None:
        pipeline_context = parse_end_of_night_command_line_arguments()
    logs.start_logging(log_level=pipeline_context.log_level)
    stages_to_do = [munge.DataMunger, crosstalk.CrosstalkCorrector, bias.OverscanSubtractor,
                    gain.GainNormalizer, mosaic.MosaicCreator, trim.Trimmer, bias.BiasSubtractor,
                    dark.DarkSubtractor, flats.FlatMaker, headers.HeaderUpdater]
    run(stages_to_do, pipeline_context, image_types=['SKYFLAT'], calibration_maker=True,
        log_message='Making Master Flat')
    logs.stop_logging()


def reduce_science_frames(pipeline_context=None):
    if pipeline_context is None:
        pipeline_context = parse_end_of_night_command_line_arguments()

    logs.start_logging(log_level=pipeline_context.log_level)
    stages_to_do = [munge.DataMunger, crosstalk.CrosstalkCorrector, bias.OverscanSubtractor,
                    gain.GainNormalizer, mosaic.MosaicCreator, trim.Trimmer, bias.BiasSubtractor,
                    dark.DarkSubtractor, flats.FlatDivider, photometry.SourceDetector,
                    astrometry.WCSSolver, headers.HeaderUpdater]

    image_list = image_utils.make_image_list(pipeline_context)
    for image in image_list:
        pipeline_context.filename = os.path.basename(image)
        run(stages_to_do, pipeline_context, image_types=['EXPOSE', 'STANDARD'],
            log_message='Reducing Science Frames')
    logs.stop_logging()


def create_master_calibrations():
    make_master_bias()
    make_master_dark()
    make_master_flat()

    
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

    pipeline_context = PipelineContext(args)

    # Ping the configdb to get currently schedulable telescopes
    try:
        dbs.populate_telescope_tables(pipeline_context.db_address)
    except Exception as e:
        logger.error('Could not connect to the configdb.')
        logger.error(e)

    timezone = dbs.get_timezone(args.site, db_address=args.db_address)

    telescopes = dbs.get_scheduable_telescopes(args.site, db_address=args.db_address)

    if timezone is not None:
        # If no dayobs is given, calculate it.
        if args.dayobs is None:
            args.dayobs = date_utils.get_dayobs(timezone=timezone)

        # For each telescope at the given site
        for telescope in telescopes:
            pipeline_context.raw_path = os.path.join(args.rawpath_root, args.site,
                                                     telescope.instrument, args.dayobs, 'raw')
            # Run the reductions on the given dayobs
            make_master_bias(pipeline_context=pipeline_context)
            make_master_dark(pipeline_context=pipeline_context)
            make_master_flat(pipeline_context=pipeline_context)
            reduce_science_frames(pipeline_context=pipeline_context)


def parse_end_of_night_command_line_arguments():
    parser = argparse.ArgumentParser(
        description='Make master calibration frames from LCOGT imaging data.')
    parser.add_argument("--raw-path", dest='raw_path', default='/archive/engineering',
                        help='Top level directory where the raw data is stored')
    parser.add_argument("--processed-path", default='/archive/engineering/',
                        help='Top level directory where the processed data will be stored')
    parser.add_argument("--log-level", default='info', choices=['debug', 'info', 'warning',
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

    return PipelineContext(args)


def run(stages_to_do, pipeline_context, image_types=[], calibration_maker=False, log_message=''):
    """
    Main driver script for banzai.
    """
    logger.info(log_message)

    image_list = image_utils.make_image_list(pipeline_context)
    image_list = image_utils.select_images(image_list, image_types)
    images = banzai.images.read_images(image_list, pipeline_context)

    for stage in stages_to_do:
        stage_to_run = stage(pipeline_context)
        images = stage_to_run.run(images)

    image_utils.save_images(pipeline_context, images, master_calibration=calibration_maker)


def run_preview_pipeline():
    parser = argparse.ArgumentParser(
        description='Make master calibration frames from LCOGT imaging data.')

    parser.add_argument("--processed-path", default='/archive/engineering',
                        help='Top level directory where the processed data will be stored')
    parser.add_argument("--log-level", default='info', choices=['debug', 'info', 'warning',
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
                        help='Number of listener processes to spawn.')

    parser.add_argument('--broker-url', dest='broker_url',
                        default='amqp://guest:guest@cerberus.lco.gtn',
                        help='URL for the broker service.')
    parser.add_argument('--queue-name', dest='queue_name', default='preview_pipeline',
                        help='Name of the queue to listen to from the fits exchange.')
    args = parser.parse_args()
    args.preview_mode = True
    args.raw_path = None
    args.filename = None
    pipeline_context = PipelineContext(args)

    try:
        dbs.populate_telescope_tables(pipeline_context.db_address)
    except Exception as e:
        logger.error('Could not connect to the configdb.')
        logger.error(e)

    logs.start_logging(log_level=pipeline_context.log_level)
    logger.info('Starting pipeline preview mode listener')

    for i in range(args.n_processes):
        p = multiprocessing.Process(target=run_indiviudal_listener, args=(args.broker_url,
                                                                          args.queue_name,
                                                                          PipelineContext(args)))
        p.start()

    logs.stop_logging()


def run_indiviudal_listener(broker_url, queue_name, pipeline_context):
    crawl_exchange = Exchange('fits_files', type='fanout')

    listener = PreviewModeListener(broker_url, pipeline_context)

    with Connection(listener.broker_url) as connection:
        listener.connection = connection
        listener.queue = Queue(queue_name, crawl_exchange)
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
        consumer = Consumer(queues=[self.queue], callbacks=[self.on_message])
        # Only fetch one thing off the queue at a time
        consumer.qos(prefetch_count=1)
        return [consumer]

    def on_message(self, body, message):
        path = body.get('path')
        if 'e00.fits' in path or 's00.fits' in path:
            if not dbs.preview_file_already_processed(path, db_address=self.pipeline_context.db_address):
                stages_to_do = [munge.DataMunger, crosstalk.CrosstalkCorrector,
                                bias.OverscanSubtractor, gain.GainNormalizer, mosaic.MosaicCreator,
                                trim.Trimmer, bias.BiasSubtractor, dark.DarkSubtractor,
                                flats.FlatDivider, photometry.SourceDetector, astrometry.WCSSolver,
                                headers.HeaderUpdater]
                logger.info('Running preview reduction on {}'.format(path))
                self.pipeline_context.filename = os.path.basename(path)
                self.pipeline_context.raw_path = os.path.dirname(path)
                run(stages_to_do, self.pipeline_context, image_types=['EXPOSE', 'STANDARD'])
                dbs.set_preview_file_as_processed(path, db_address=self.pipeline_context.db_address)

        message.ack()  # acknowledge to the sender we got this message (it can be popped)
