"""
Site Cache Initialization for Docker Compose Deployments

This module provides one-time initialization for site deployment with
PostgreSQL replication cache. Designed to run as a Docker Compose init
container that completes successfully before other services start.

Usage as init container:
    command: ["python", "-m", "banzai.cache.init"]

The init container pattern is used because:
1. Database schema is defined in Python (SQLAlchemy), not SQL files
2. Avoids duplicating schema definitions
3. All initialization logic is testable
4. Standard practice for ORM-based applications
"""

import os
import sys

from banzai import dbs
from banzai.logs import get_logger
from banzai.cache import replication

logger = get_logger()


def is_already_initialized(db_address):
    """
    Check if the database is already initialized.

    Returns True if cache_config table exists and has a record,
    indicating initialization has already been completed.
    """
    try:
        config = dbs.get_cache_config(db_address)
        return config is not None
    except Exception:
        # Table doesn't exist or other error
        return False


def check_subscription_exists(db_address):
    """
    Check if a replication subscription already exists.
    """
    try:
        subscriptions = replication.get_subscription_status(db_address)
        return len(subscriptions) > 0
    except Exception:
        return False


def run_initialization():
    """
    Run one-time site cache initialization.

    Designed to be run as a Docker Compose init service.
    Exits 0 on success (including if already initialized), 1 on failure.

    Environment Variables Required:
        DB_ADDRESS: Local PostgreSQL database address
        AWS_DB_ADDRESS: AWS PostgreSQL connection string for replication
        SITE_ID: Site identifier (e.g., 'lsc', 'ogg')

    Environment Variables Optional:
        INSTRUMENT_TYPES_TO_CACHE: Comma-separated types or '*' for all (default: '*')
        CACHE_FILES_ROOT: Root directory for cached files (default: '/data/calibrations')
        PUBLICATION_NAME: AWS publication name (default: 'banzai_calibrations')
    """
    try:
        # Read configuration from environment
        db_address = os.getenv('DB_ADDRESS')
        aws_db_address = os.getenv('AWS_DB_ADDRESS')
        site_id = os.getenv('SITE_ID')
        instrument_types = os.getenv('INSTRUMENT_TYPES_TO_CACHE', '*')
        cache_root = os.getenv('CACHE_FILES_ROOT', '/data/calibrations')
        publication_name = os.getenv('PUBLICATION_NAME', 'banzai_calibrations')

        # Validate required environment variables
        if not db_address:
            logger.error("DB_ADDRESS environment variable is required")
            sys.exit(1)

        if not site_id:
            logger.error("SITE_ID environment variable is required")
            sys.exit(1)

        logger.info(f"Starting site cache initialization for site: {site_id}")
        logger.info(f"  Database: {db_address}")
        logger.info(f"  Instrument types: {instrument_types}")
        logger.info(f"  Cache root: {cache_root}")

        # Check if already initialized
        if is_already_initialized(db_address):
            logger.info("Cache already initialized, skipping initialization")
            sys.exit(0)

        # Step 1: Create database schema
        logger.info("Step 1/4: Creating database schema...")
        dbs.create_db(db_address)
        logger.info("  Database schema created successfully")

        # Step 2: Set up replication subscription (if AWS address provided)
        if aws_db_address:
            logger.info("Step 2/4: Setting up replication subscription...")
            if check_subscription_exists(db_address):
                logger.info("  Replication subscription already exists, skipping")
            else:
                replication.setup_subscription(
                    local_db_address=db_address,
                    aws_connection_string=aws_db_address,
                    site_id=site_id,
                    publication_name=publication_name
                )
                logger.info("  Replication subscription created successfully")
        else:
            logger.info("Step 2/4: Skipping replication setup (AWS_DB_ADDRESS not provided)")
            logger.info("  Running in local-only mode without replication")

        # Step 3: Install triggers
        logger.info("Step 3/4: Installing database triggers...")
        replication.install_triggers(db_address)
        logger.info("  Filepath preservation trigger installed successfully")

        # Step 4: Initialize cache configuration
        logger.info("Step 4/4: Initializing cache configuration...")
        instrument_types_list = (
            instrument_types.split(',') if instrument_types != '*' else ['*']
        )
        dbs.initialize_cache_config(
            db_address=db_address,
            site_id=site_id,
            instrument_types=instrument_types_list,
            cache_root=cache_root
        )
        logger.info("  Cache configuration initialized successfully")

        logger.info("=" * 60)
        logger.info("Site cache initialization completed successfully!")
        logger.info("=" * 60)
        sys.exit(0)

    except Exception as e:
        logger.error(f"Initialization failed: {e}", exc_info=True)
        sys.exit(1)


def main():
    """Entry point for console script and module execution."""
    run_initialization()


if __name__ == '__main__':
    main()
