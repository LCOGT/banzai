""" main.py: Main driver script for banzai.

    The main() function is a console entry point.

Author
    Curtis McCully (cmccully@lcogt.net)

October 2015
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import argparse

import banzai.images
from banzai.utils import image_utils
from banzai import munge, crosstalk, gain, mosaic
from banzai import bias, dark, flats, trim, photometry, astrometry, headers, qc
from banzai import logs
from banzai.utils import file_utils
from banzai import dbs
import os
import sys
from kombu.mixins import ConsumerMixin
from kombu import Connection, Queue, Exchange

# A dictionary converting the string input by the user into the corresponding Python object
reduction_stages = [bias.BiasMaker]

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


def make_master_bias(cmd_args=None):
    pipeline_context = parse_command_line_arguments(cmd_args=cmd_args)
    logs.start_logging(log_level=pipeline_context.log_level)
    stages_to_do = [munge.DataMunger, qc.SaturationTest, crosstalk.CrosstalkCorrector, bias.OverscanSubtractor,
                    gain.GainNormalizer, mosaic.MosaicCreator, trim.Trimmer, bias.BiasMaker,
                    headers.HeaderUpdater]
    run(stages_to_do, pipeline_context, image_types=['BIAS'], calibration_maker=True,
        log_message='Making Master BIAS')
    logs.stop_logging()


def make_master_dark(cmd_args=None):
    pipeline_context = parse_command_line_arguments(cmd_args=cmd_args)
    logs.start_logging(log_level=pipeline_context.log_level)
    stages_to_do = [munge.DataMunger, qc.SaturationTest, crosstalk.CrosstalkCorrector, bias.OverscanSubtractor,
                    gain.GainNormalizer, mosaic.MosaicCreator, trim.Trimmer,
                    bias.BiasSubtractor, dark.DarkMaker, headers.HeaderUpdater]
    run(stages_to_do, pipeline_context, image_types=['DARK'], calibration_maker=True,
        log_message='Making Master Dark')
    logs.stop_logging()


def make_master_flat(cmd_args=None):
    pipeline_context = parse_command_line_arguments(cmd_args=cmd_args)
    logs.start_logging(log_level=pipeline_context.log_level)
    stages_to_do = [munge.DataMunger, qc.SaturationTest, crosstalk.CrosstalkCorrector, bias.OverscanSubtractor,
                    gain.GainNormalizer, mosaic.MosaicCreator, trim.Trimmer, bias.BiasSubtractor,
                    dark.DarkSubtractor, flats.FlatMaker, headers.HeaderUpdater]
    run(stages_to_do, pipeline_context, image_types=['SKYFLAT'], calibration_maker=True,
        log_message='Making Master Flat')
    logs.stop_logging()


def reduce_science_frames(cmd_args=None):

    pipeline_context = parse_command_line_arguments(cmd_args=cmd_args)
    logs.start_logging(log_level=pipeline_context.log_level)

    stages_to_do = [munge.DataMunger, qc.SaturationTest, crosstalk.CrosstalkCorrector, bias.OverscanSubtractor,
                    gain.GainNormalizer, mosaic.MosaicCreator, trim.Trimmer, bias.BiasSubtractor,
                    dark.DarkSubtractor, flats.FlatDivider, photometry.SourceDetector,
                    astrometry.WCSSolver, headers.HeaderUpdater]

    image_list = image_utils.make_image_list(pipeline_context)
    for image in image_list:
        pipeline_context.filename = os.path.basename(image)
        run(stages_to_do, pipeline_context, image_types=['EXPOSE', 'STANDARD'],
            log_message='Reducing Science Frames')
    logs.stop_logging()


def create_master_calibrations(cmd_args=None):
    make_master_bias(cmd_args=cmd_args)
    make_master_dark(cmd_args=cmd_args)
    make_master_flat(cmd_args=cmd_args)

    
def reduce_night(cmd_args=None):
    make_master_bias(cmd_args=cmd_args)
    make_master_dark(cmd_args=cmd_args)
    make_master_flat(cmd_args=cmd_args)
    reduce_science_frames(cmd_args=cmd_args)


def parse_command_line_arguments(cmd_args=None):
    parser = argparse.ArgumentParser(
        description='Make master calibration frames from LCOGT imaging data.')
    parser.add_argument("--raw-path", default='/archive/engineering',
                        help='Top level directory where the raw data is stored')
    parser.add_argument("--processed-path", default='/nethome/supernova/banzai',
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
    parser.add_argument('--preview-mode', dest='preview_mode', action='store_true',
                        help='Store the data preview mode?')

    parser.add_argument('--filename', dest='filename', default=None,
                        help='Filename of the image to reduce.')
    args = parser.parse_args(cmd_args)

    return PipelineContext(args)


def run(stages_to_do, pipeline_context, image_types=[], calibration_maker=False, log_message=''):
    """
    Main driver script for banzai.
    """
    logger.info(log_message)

    image_list = image_utils.make_image_list(pipeline_context)
    image_list = image_utils.select_images(image_list, image_types)
    images = banzai.utils.image_utils.read_images(image_list, pipeline_context)

    for stage in stages_to_do:
        stage_to_run = stage(pipeline_context)
        images = stage_to_run.run(images)

    image_utils.save_images(pipeline_context, images, master_calibration=calibration_maker)


def run_preview_pipeline(cmd_args=None):
    pipeline_context = parse_command_line_arguments(cmd_args=cmd_args)
    logs.start_logging(log_level=pipeline_context.log_level)
    logger.info('Starting pipeline preview mode listener')
    crawl_exchange = Exchange('fits_files', type='fanout')

    listener = PreviewModeListener('amqp://guest:guest@cerberus.lco.gtn', pipeline_context)

    with Connection(listener.broker_url) as connection:
        listener.connection = connection
        listener.queue = Queue('preview_pipeline', crawl_exchange)
        try:
            listener.run()
        except KeyboardInterrupt:
            logger.info('Shutting down preview pipeline listener...')
            logs.stop_logging()
            sys.exit(0)


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
                stages_to_do = [munge.DataMunger, qc.SaturationTest, crosstalk.CrosstalkCorrector,
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
