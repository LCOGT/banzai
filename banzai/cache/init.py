"""Runs as a Docker Compose init container to set up local DB schema and replication."""

import os
import sys

from psycopg2 import errors as pg_errors
from sqlalchemy.exc import ProgrammingError

from banzai import dbs, logs
from banzai.cache import replication

logger = logs.get_logger()


def run_initialization():
    """
    Environment Variables:
        DB_ADDRESS: Local PostgreSQL database address (required)
        AWS_DB_ADDRESS: AWS PostgreSQL connection string for replication (optional)
        SITE_ID: Site identifier, e.g. 'lsc' (required if AWS_DB_ADDRESS is set)
    """
    db_address = os.getenv('DB_ADDRESS')
    aws_db_address = os.getenv('AWS_DB_ADDRESS')
    site_id = os.getenv('SITE_ID')

    if not db_address:
        logger.error("DB_ADDRESS environment variable is required")
        sys.exit(1)

    if aws_db_address and not site_id:
        logger.error("SITE_ID environment variable is required when AWS_DB_ADDRESS is set")
        sys.exit(1)

    try:
        logger.info("Creating database schema...")
        dbs.create_db(db_address)
        logger.info("Database schema created successfully")

        if aws_db_address:
            logger.info("Setting up replication subscription...")
            try:
                replication.setup_subscription(db_address, aws_db_address, site_id=site_id)
                logger.info("Replication subscription created successfully")
            except (ProgrammingError, pg_errors.DuplicateObject):
                logger.info("Replication subscription already exists, skipping")
        else:
            logger.info("AWS_DB_ADDRESS not set, skipping replication setup")

        logger.info("Initialization completed successfully")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Initialization failed: {e}", exc_info=True)
        sys.exit(1)
