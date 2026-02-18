"""
Site Cache Initialization for Docker Compose Deployments

Runs as a Docker Compose init container to set up the local database schema
and optional PostgreSQL replication subscription before other services start.
"""

import os
import sys

from banzai import dbs, logs
from banzai.cache import replication

logger = logs.get_logger()


def run_initialization():
    """
    Run one-time site cache initialization.

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
        # Step 1: Create database schema
        logger.info("Creating database schema...")
        dbs.create_db(db_address)
        logger.info("Database schema created successfully")

        # Step 2: Set up replication subscription (if AWS address provided)
        if aws_db_address:
            logger.info("Setting up replication subscription...")
            try:
                replication.setup_subscription(db_address, aws_db_address, site_id=site_id)
                logger.info("Replication subscription created successfully")
            except Exception as e:
                logger.info(f"Replication subscription already set up (skipping): {e}")
        else:
            logger.info("AWS_DB_ADDRESS not set, skipping replication setup")

        logger.info("Initialization completed successfully")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Initialization failed: {e}", exc_info=True)
        sys.exit(1)


def main():
    """Entry point for console script."""
    run_initialization()


if __name__ == '__main__':
    main()
