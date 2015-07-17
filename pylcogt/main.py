from __future__ import absolute_import, print_function

import argparse

import os
import itertools

import sqlalchemy
from .utils import date_utils
from . import ingest
from . import dbs
from . import logs
from . import bias


reduction_stages = ['ingest', 'make_bias', 'make_flat', 'make_dark', 'subtract_bias', 'trim',
                    'apply_dark', 'apply_flat', 'cr_reject', 'wcs', 'check_image', 'hdu_update']

def get_telescope_info():
   # Get All of the telescope information
    db_session = dbs.get_session()
    all_sites = []
    for site in db_session.query(dbs.Telescope.site).distinct():
        all_sites.append(site[0])

    all_instruments = []
    for instrument in db_session.query(dbs.Telescope.instrument).distinct():
        all_instruments.append(instrument[0])

    all_telescope_ids = []
    for telescope_id in db_session.query(dbs.Telescope.telescope_id).distinct():
        all_telescope_ids.append(telescope_id[0])

    all_camera_types = []
    for camera_type in db_session.query(dbs.Telescope.camera_type).distinct():
        all_camera_types.append(camera_type[0])

    db_session.close()
    return all_sites, all_instruments, all_telescope_ids, all_camera_types


def main():
    # Get the telescope info
    all_sites, all_instruments, all_telescope_ids, all_camera_types = get_telescope_info()

    parser = argparse.ArgumentParser(description='Reduce LCOGT imaging data.')
    parser.add_argument("--epoch", required=True, type=str, help='Epoch to reduce')
    parser.add_argument("--telescope", default='', choices=all_telescope_ids,
                        help='Telescope ID (e.g. 1m0-010).')
    parser.add_argument("--instrument", default='', type=str, choices=all_instruments,
                        help='Instrument code (e.g. kb74)')
    parser.add_argument("--site", default='', type=str, choices=all_sites,
                        help='Site code (e.g. elp)')
    parser.add_argument("--camera-type", default='', type=str, choices=all_camera_types,
                        help='Camera type (e.g. sbig)')

    parser.add_argument("--stage", default='', choices=reduction_stages,
                        help='Reduction stages to run')

    parser.add_argument("--raw-path", default='/archive/engineering',
                        help='Top level directory where the raw data is stored')
    parser.add_argument("--processed-path", default='/nethome/supernova/pylcogt',
                        help='Top level directory where the processed data will be stored')

    parser.add_argument("--filter", default='', help="Image filter",
                        choices=['sloan','landolt', 'apass','up','gp','rp','ip','zs',
                                 'U','B','V','R','I'])
    parser.add_argument("--binning", default='', choices=['1x1', '2x2'],
                        help="Image binning (CCDSUM)")
    parser.add_argument("--image-type", default='', choices=['BIAS', 'DARK', 'SKYFLAT', 'EXPOSE'],
                        help="Image type to reduce.")

    parser.add_argument("--log-level", default='info', choices=['debug', 'info', 'warning',
                                                                'critical', 'fatal', 'error'])
    # parser.add_argument("-n", "--name", dest="name", default='', type="str",
    #                   help='-n image name   \t [%default]')
    # parser.add_argument("-d", "--id", dest="id", default='', type="str",
    #                   help='-d identification id   \t [%default]')

    args = parser.parse_args()

    logs.start_logging(log_level=args.log_level)
    epoch_list = date_utils.parse_epoch_string(args.epoch)

    if args.stage != '':
        stages_to_do = [args.stage]
    else:
        stages_to_do = reduction_stages

    # Get the telescopes for which we want to reduce data.
    db_session = dbs.get_session()

    telescope_query = sqlalchemy.sql.expression.true()

    if args.site != '':
        telescope_query &= dbs.Telescope.site == args.site

    if args.instrument != '':
        telescope_query &= dbs.Telescope.instrument == args.instrument

    if args.telescope != '':
        telescope_query &= dbs.Telescope.telescope_id == args.telescope

    if args.camera_type != '':
        telescope_query &= dbs.Telescope.camera_type == args.camera_type

    telescope_list = db_session.query(dbs.Telescope).filter(telescope_query).all()

    image_query = sqlalchemy.sql.expression.true()

    if args.filter != '':
        image_query &= dbs.Image.filter_name == args.filter

    if args.binning != '':
        ccdsum = args.binning.replace('x', ' ')
        image_query &= dbs.Image.ccdsum == ccdsum

    logger = logs.get_logger('Main')
    logger.info('Starting pylcogt:')

    if 'ingest' in stages_to_do:
        ingest_stage = ingest.Ingest(args.raw_path, args.processed_path, image_query)
        ingest_stage.run(epoch_list, telescope_list)

    if 'make_bias' in stages_to_do:
        make_bias = bias.MakeBias(image_query, args.processed_path)
        make_bias.run(epoch_list, telescope_list)

    if 'subtract_bias' in stages_to_do:
        subtract_bias = bias.SubtractBias(image_query, args.processed_path)
        subtract_bias.run(epoch_list, telescope_list)

    db_session.close()
    logs.stop_logging()
