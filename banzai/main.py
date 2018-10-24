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
import operator

from kombu import Exchange, Connection, Queue
from kombu.mixins import ConsumerMixin

from banzai.context import PipelineContext
import banzai.images
from banzai import bias, dark, flats, trim, photometry, astrometry, qc, logs
from banzai import dbs
from banzai import crosstalk, gain, mosaic, bpm
from banzai import preview
from banzai.qc import pointing
from banzai.utils import image_utils, date_utils
from banzai.context import TelescopeCriterion

logger = logging.getLogger(__name__)

ORDERED_STAGES = [qc.HeaderSanity,
                  qc.ThousandsTest,
                  qc.SaturationTest,
                  bias.OverscanSubtractor,
                  crosstalk.CrosstalkCorrector,
                  gain.GainNormalizer,
                  mosaic.MosaicCreator,
                  bpm.BPMUpdater,
                  trim.Trimmer,
                  bias.BiasSubtractor,
                  dark.DarkSubtractor,
                  flats.FlatDivider,
                  qc.PatternNoiseDetector,
                  photometry.SourceDetector,
                  astrometry.WCSSolver,
                  pointing.PointingTest]

IMAGING_CRITERIA = [TelescopeCriterion('camera_type', operator.contains, 'FLOYDS', exclude=True),
                    TelescopeCriterion('camera_type', operator.contains, 'NRES', exclude=True),
                    TelescopeCriterion('schedulable', operator.eq, True)]

PREVIEW_ELIGIBLE_SUFFIXES = ['e00.fits', 's00.fits', 'b00.fits', 'd00.fits', 'f00.fits']


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
        last_index = ORDERED_STAGES.index(last_stage) + 1

    stages_todo = ORDERED_STAGES[:last_index] + extra_stages
    return stages_todo


def parse_args(parser):
    """Parse arguments, including default command line argument, and set the overall log level"""
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
    parser.add_argument('--rlevel', dest='rlevel', default=91, help='Reduction level')
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
    args = parser.parse_args()

    logs.set_log_level(args.log_level)

    return args


def run(stages_to_do, image_paths, pipeline_context, calibration_maker=False):
    """
    Main driver script for banzai.
    """
    images = banzai.images.read_images(image_paths, pipeline_context)

    for stage in stages_to_do:
        stage_to_run = stage(pipeline_context)
        images = stage_to_run.run(images)

    output_files = image_utils.save_images(pipeline_context, images, master_calibration=calibration_maker)
    return output_files


def parse_end_of_night_command_line_arguments(selection_criteria):
    parser = argparse.ArgumentParser(description='Process LCO data.')
    parser.add_argument("--raw-path", dest='raw_path', default='/archive/engineering',
                        help='Top level directory where the raw data is stored')
    args = parse_args(parser)
    raw_path = args.raw_path
    delattr(args, 'raw_path')
    return PipelineContext(args, selection_criteria), raw_path


def process_directory(selection_criteria, image_types=None, last_stage=None, extra_stages=None, log_message='',
                      calibration_maker=False, raw_path=None):
    parser = argparse.ArgumentParser(description='Process LCO data.')
    if raw_path is None:
        parser.add_argument("--raw-path", dest='raw_path', default='/archive/engineering',
                            help='Top level directory where the raw data is stored')
    args = parse_args(parser)
    if raw_path is None:
        raw_path = args.raw_path
        delattr(args, 'raw_path')

    pipeline_context = PipelineContext(args, selection_criteria)

    if len(log_message) > 0:
        logger.info(log_message, raw_path=raw_path)
    stages_to_do = get_stages_todo(last_stage, extra_stages=extra_stages)
    image_list = image_utils.make_image_list(raw_path)
    image_list = image_utils.select_images(image_list, image_types,
                                           pipeline_context.allowed_instrument_criteria,
                                           db_address=pipeline_context.db_address)
    if calibration_maker:
        try:
            run(stages_to_do, image_list, pipeline_context, calibration_maker=True)
        except Exception as e:
            logger.error(e, raw_path=raw_path)
    else:
        for image in image_list:
            try:
                run(stages_to_do, [image], pipeline_context, calibration_maker=False)
            except Exception as e:
                logger.error(e, filename=image)


def make_master_bias(raw_path=None):
    process_directory(IMAGING_CRITERIA, ['BIAS'], last_stage=trim.Trimmer,
                      extra_stages=[bias.BiasMasterLevelSubtractor, bias.BiasComparer, bias.BiasMaker],
                      log_message='Making Master BIAS', calibration_maker=True, raw_path=raw_path)


def make_master_dark(raw_path=None):
    process_directory(IMAGING_CRITERIA, ['DARK'], last_stage=bias.BiasSubtractor,
                      extra_stages=[dark.DarkNormalizer, dark.DarkComparer, dark.DarkMaker],
                      log_message='Making Master Dark', calibration_maker=True, raw_path=raw_path)


def make_master_flat():
    process_directory(IMAGING_CRITERIA, ['SKYFLAT'], last_stage=dark.DarkSubtractor, log_message='Making Master Flat',
                      extra_stages=[flats.FlatNormalizer, qc.PatternNoiseDetector, flats.FlatComparer, flats.FlatMaker],
                      calibration_maker=True, raw_path=raw_path)


def reduce_science_frames(raw_path=None):
    process_directory(IMAGING_CRITERIA, ['EXPOSE', 'STANDARD'], raw_path=raw_path)


def reduce_experimental_frames(raw_path=None):
    process_directory(IMAGING_CRITERIA, ['EXPERIMENTAL'], raw_path=raw_path)


def reduce_trailed_frames(raw_path=None):
    process_directory(IMAGING_CRITERIA, ['TRAILED'], raw_path=raw_path)


def preprocess_sinistro_frames(raw_path=None):
    process_directory(IMAGING_CRITERIA, ['EXPOSE', 'STANDARD', 'BIAS', 'DARK', 'SKYFLAT', 'TRAILED', 'EXPERIMENTAL'],
                      last_stage=mosaic.MosaicCreator, raw_path=raw_path)


def reduce_night():
    parser = argparse.ArgumentParser(description='Reduce all the data from a site at the end of a night.')
    parse_args(parser)
    parser.add_argument('--site', dest='site', help='Site code (e.g. ogg)')
    parser.add_argument('--dayobs', dest='dayobs',
                        default=None, help='Day-Obs to reduce (e.g. 20160201)')
    parser.add_argument('--raw-path-root', dest='rawpath_root', default='/archive/engineering',
                        help='Top level directory with raw data.')
    args = parse_args(parser)

    pipeline_context = PipelineContext(args, IMAGING_CRITERIA)

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
            raw_path = os.path.join(args.rawpath_root, args.site, telescope.instrument, args.dayobs, 'raw')

            # Run the reductions on the given dayobs
            try:
                make_master_bias(raw_path=raw_path)
            except Exception as e:
                logger.error(e)
            try:
                make_master_dark(raw_path=raw_path)
            except Exception as e:
                logger.error(e)
            try:
                make_master_flat(raw_path=raw_path)
            except Exception as e:
                logger.error(e)
            try:
                reduce_science_frames(raw_path=raw_path)
            except Exception as e:
                logger.error(e)


def get_preview_stages_todo(image_suffix):
    if image_suffix == 'b00.fits':
        stages = get_stages_todo(last_stage=trim.Trimmer,
                                 extra_stages=[bias.BiasMasterLevelSubtractor, bias.BiasComparer])
    elif image_suffix == 'd00.fits':
        stages = get_stages_todo(last_stage=bias.BiasSubtractor,
                                 extra_stages=[dark.DarkNormalizer, dark.DarkComparer])
    elif image_suffix == 'f00.fits':
        stages = get_stages_todo(last_stage=dark.DarkSubtractor,
                                 extra_stages=[flats.FlatNormalizer, qc.PatternNoiseDetector, flats.FlatComparer])
    else:
        stages = get_stages_todo()
    return stages


def run_preview_pipeline():
    parser = argparse.ArgumentParser(
        description='Make master calibration frames from LCOGT imaging data.')
    parser.add_argument('--n-processes', dest='n_processes', default=12,
                        help='Number of listener processes to spawn.', type=int)
    parser.add_argument('--broker-url', dest='broker_url',
                        default='amqp://guest:guest@rabbitmq.lco.gtn:5672/',
                        help='URL for the broker service.')
    parser.add_argument('--queue-name', dest='queue_name', default='preview_pipeline',
                        help='Name of the queue to listen to from the fits exchange.')

    args = parse_args(parser)

    pipeline_context = PipelineContext(args, IMAGING_CRITERIA)

    try:
        dbs.populate_telescope_tables(db_address=pipeline_context.db_address)
    except Exception as e:
        logger.error('Could not connect to the configdb.')
        logger.error(e)

    logger.info('Starting pipeline preview mode listener')

    for i in range(args.n_processes):
        p = multiprocessing.Process(target=run_individual_listener, args=(args.broker_url,
                                                                          args.queue_name,
                                                                          PipelineContext(args, IMAGING_CRITERIA)))
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
        for suffix in PREVIEW_ELIGIBLE_SUFFIXES:
            if suffix in path:
                is_eligible_for_preview = True
                image_suffix = suffix

        if is_eligible_for_preview:
            try:
                if preview.need_to_make_preview(path, self.pipeline_context.allowed_instrument_criteria,
                                                db_address=self.pipeline_context.db_address,
                                                max_tries=self.pipeline_context.max_tries):
                    stages_to_do = get_preview_stages_todo(image_suffix)

                    logger.info('Running preview reduction on {}'.format(path),
                                filename=os.path.basename(path))

                    # Increment the number of tries for this file
                    preview.increment_preview_try_number(path, db_address=self.pipeline_context.db_address)

                    run(stages_to_do, [path], self.pipeline_context)
                    preview.set_preview_file_as_processed(path, db_address=self.pipeline_context.db_address)

            except Exception:
                exc_type, exc_value, exc_tb = sys.exc_info()
                tb_msg = traceback.format_exception(exc_type, exc_value, exc_tb)

                logger.error("Exception producing preview frame. {path}. {error}".format(path=path, error=tb_msg),
                             filename=os.path.basename(path))
