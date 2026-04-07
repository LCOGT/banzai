"""Runs as a Docker Compose init container to set up local DB schema and replication."""

import argparse
import sys

from sqlalchemy.exc import ProgrammingError

from banzai import dbs, logs
from banzai.cache import replication

logger = logs.get_logger()


def create_parser():
    parser = argparse.ArgumentParser(description='Initialize local database and optional replication.')
    parser.add_argument('--db-address', dest='db_address', required=True,
                        help='Local PostgreSQL database address')
    parser.add_argument('--aws-db-address', dest='aws_db_address', default=None,
                        help='AWS PostgreSQL connection string for replication')
    parser.add_argument('--site-id', dest='site_id', default=None,
                        help='Site identifier (e.g. lsc). Required when --aws-db-address is set.')
    parser.add_argument('--publication-name', dest='publication_name', default='banzai_calibrations',
                        help='AWS publication name (default: banzai_calibrations)')
    parser.add_argument('--slot-name', dest='slot_name', default=None,
                        help='Replication slot name (default: banzai_<site-id>_slot)')
    return parser


def run_initialization():
    args = create_parser().parse_args()

    if args.aws_db_address and not args.site_id:
        logger.error("--site-id is required when --aws-db-address is set")
        sys.exit(1)

    try:
        logger.info("Creating database schema...")
        dbs.create_db(args.db_address)
        logger.info("Database schema created successfully")

        if args.aws_db_address:
            logger.info("Setting up replication subscription...")
            try:
                replication.setup_subscription(args.db_address, args.aws_db_address,
                                               site_id=args.site_id,
                                               publication_name=args.publication_name,
                                               slot_name=args.slot_name)
                logger.info("Replication subscription created successfully")
            except ProgrammingError:
                logger.info("Replication subscription already exists, skipping")
        else:
            logger.info("--aws-db-address not provided, skipping replication setup")

        logger.info("Initialization completed successfully")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Initialization failed: {e}", exc_info=True)
        sys.exit(1)
