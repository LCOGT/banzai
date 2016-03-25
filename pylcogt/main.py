""" main.py: Main driver script for PyLCOGT.

    The main() function is a console entry point.

Author
    Curtis McCully (cmccully@lcogt.net)

October 2015
"""
from __future__ import absolute_import, print_function, division

import argparse

from pylcogt import munge, crosstalk, gain, mosaic
from pylcogt import bias, dark, flats, trim, photometry, astrometry, headers
from pylcogt import dbs, logs
from pylcogt.utils import file_utils

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


def make_master_bias(cmd_args=None):
    stages_to_do = [munge.DataMunger, crosstalk.CrosstalkCorrector, bias.OverscanSubtractor,
                    gain.GainNormalizer, mosaic.MosaicCreator, trim.Trimmer, bias.BiasMaker,
                    headers.HeaderUpdater]
    run(stages_to_do=stages_to_do, image_type='BIAS', calibration_maker=True,
        log_message='Making Master BIAS', cmd_args=cmd_args)


def make_master_dark(cmd_args=None):
    stages_to_do = [munge.DataMunger, crosstalk.CrosstalkCorrector, bias.OverscanSubtractor,
                    gain.GainNormalizer, mosaic.MosaicCreator, trim.Trimmer,
                    bias.BiasSubtractor, dark.DarkMaker, headers.HeaderUpdater]
    run(stages_to_do=stages_to_do, image_type='DARK', calibration_maker=True,
        log_message='Making Master Dark', cmd_args=cmd_args)


def make_master_flat(cmd_args=None):
    stages_to_do = [munge.DataMunger, crosstalk.CrosstalkCorrector, bias.OverscanSubtractor,
                    gain.GainNormalizer, mosaic.MosaicCreator, trim.Trimmer, bias.BiasSubtractor,
                    dark.DarkSubtractor, flats.FlatMaker, headers.HeaderUpdater]
    run(stages_to_do=stages_to_do, image_type='SKYFLAT', calibration_maker=True,
        log_message='Making Master Flat', cmd_args=cmd_args)


def reduce_science_frames(cmd_args=None):
    stages_to_do = [munge.DataMunger, crosstalk.CrosstalkCorrector, bias.OverscanSubtractor,
                    gain.GainNormalizer, mosaic.MosaicCreator, trim.Trimmer, bias.BiasSubtractor,
                    dark.DarkSubtractor, flats.FlatDivider, photometry.SourceDetector,
                    astrometry.WCSSolver, headers.HeaderUpdater]
    run(stages_to_do=stages_to_do, image_type='EXPOSE', log_message='Reducing Science Frames',
        cmd_args=cmd_args)


def create_master_calibrations(cmd_args=None):
    make_master_bias(cmd_args=cmd_args)
    make_master_dark(cmd_args=cmd_args)
    make_master_flat(cmd_args=cmd_args)

    
def reduce_night(cmd_args=None):
    make_master_bias(cmd_args=cmd_args)
    make_master_dark(cmd_args=cmd_args)
    make_master_flat(cmd_args=cmd_args)
    reduce_science_frames(cmd_args=cmd_args)


def run(stages_to_do, image_type='', calibration_maker=False, log_message='', cmd_args=None):
    """
    Main driver script for PyLCOGT.
    """
    # Get the available instruments/telescopes

    parser = argparse.ArgumentParser(description='Make master calibration frames from LCOGT imaging data.')
    parser.add_argument("--raw-path", default='/archive/engineering',
                        help='Top level directory where the raw data is stored')
    parser.add_argument("--processed-path", default='/nethome/supernova/pylcogt',
                        help='Top level directory where the processed data will be stored')
    parser.add_argument("--log-level", default='info', choices=['debug', 'info', 'warning',
                                                               'critical', 'fatal', 'error'])
    parser.add_argument('--post-to-archive', dest='post_to_archive', action='store_true', default=False)
    parser.add_argument('--db-address', dest='db_address',
                        default='mysql+mysqlconnector://cmccully:password@localhost/test',
                        help='Database address: Should be in SQLAlchemy form')
    parser.add_argument('--fpack', dest='fpack', action='store_true', default=False,
                        help='Fpack the output files?')
    parser.add_argument('--rlevel', dest='rlevel', default=91, help='Reduction level')

    args = parser.parse_args(cmd_args)

    logs.start_logging(log_level=args.log_level)

    logger.info(log_message)

    pipeline_context = PipelineContext(args)

    image_list = file_utils.make_image_list(pipeline_context)
    image_list = file_utils.select_images(image_list, image_type)
    images = file_utils.read_images(image_list, pipeline_context)

    for stage in stages_to_do:
        stage_to_run = stage(pipeline_context)
        images = stage_to_run.run(images)

    file_utils.save_images(pipeline_context, images, master_calibration=calibration_maker)
    # Clean up
    logs.stop_logging()
