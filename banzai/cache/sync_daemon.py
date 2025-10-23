#!/usr/bin/env python
"""
Calibration cache sync daemon.

Runs periodic syncs of calibration data from remote database to local cache.
"""
import time
import os
import sys
from banzai import logs, dbs
from banzai.cache.sync import sync_calibrations, MinimalContext

logger = logs.get_logger()


def run_sync_daemon():
    """
    Run calibration sync on startup and then every SYNC_INTERVAL seconds.

    Configuration is read from environment variables:
    - CAL_DB_ADDRESS: Source PostgreSQL database
    - CACHE_DB_ADDRESS: Local SQLite cache database
    - CACHE_FILES_ROOT: Root directory for cached files
    - SITE_ID: Site code to sync (e.g., 'lsc')
    - SYNC_INTERVAL: Seconds between syncs (default: 300 = 5 minutes)
    """
    # Configuration from environment
    source_db = os.getenv('CAL_DB_ADDRESS')
    cache_db = os.getenv('CACHE_DB_ADDRESS')
    cache_files_root = os.getenv('CACHE_FILES_ROOT', '/data/calibrations_cache/files')
    site_id = os.getenv('SITE_ID')
    sync_interval = int(os.getenv('SYNC_INTERVAL', '300'))  # 5 minutes default

    # Validate required configuration
    if not source_db:
        logger.error('CAL_DB_ADDRESS environment variable not set')
        sys.exit(1)
    if not cache_db:
        logger.error('CACHE_DB_ADDRESS environment variable not set')
        sys.exit(1)
    if not site_id:
        logger.error('SITE_ID environment variable not set')
        sys.exit(1)

    logger.info('Starting calibration cache sync daemon')
    logger.info(f'Source DB: {source_db}')
    logger.info(f'Cache DB: {cache_db}')
    logger.info(f'Cache files: {cache_files_root}')
    logger.info(f'Site: {site_id}')
    logger.info(f'Sync interval: {sync_interval} seconds')

    # Initialize cache database
    logger.info('Initializing cache database...')
    try:
        # Check if database already exists and is valid
        db_exists = False
        if 'sqlite' in cache_db and ':///' in cache_db:
            db_path = cache_db.split(':///')[-1]
            if os.path.exists(db_path):
                db_exists = True
                logger.info('Existing cache database found, validating schema...')
                try:
                    with dbs.get_session(cache_db) as db_session:
                        # Validate required tables exist by attempting simple queries
                        db_session.query(dbs.CalibrationImage).limit(1).all()
                        db_session.query(dbs.Instrument).limit(1).all()
                        db_session.query(dbs.Site).limit(1).all()
                    logger.info('Cache database schema is valid')
                except Exception as e:
                    logger.error(f'Cache database has invalid schema: {e}')
                    logger.error(f'Please remove the database file and restart:')
                    logger.error(f'  rm {db_path}')
                    sys.exit(1)

        # Only create database if it doesn't exist
        if not db_exists:
            logger.info('Creating new cache database...')
            dbs.create_local_db(cache_db, source_db, site_id)

        logger.info('Cache database ready')
    except dbs.SiteMissingException:
        logger.error(f'Site {site_id} not found in source database')
        sys.exit(1)
    except Exception as e:
        logger.error(f'Failed to initialize cache database: {e}', exc_info=True)
        sys.exit(1)

    # Create runtime context for downloading files
    runtime_context = MinimalContext()

    # Initial sync on startup
    logger.info('Running initial sync...')
    try:
        sync_calibrations(source_db, cache_db, cache_files_root, site_id, runtime_context)
        logger.info('Initial sync completed successfully')
    except Exception as e:
        logger.error(f'Initial sync failed: {e}', exc_info=True)

    # Periodic sync
    while True:
        time.sleep(sync_interval)
        logger.info('Running periodic sync...')
        try:
            sync_calibrations(source_db, cache_db, cache_files_root, site_id, runtime_context)
            logger.info('Periodic sync completed successfully')
        except Exception as e:
            logger.error(f'Periodic sync failed: {e}', exc_info=True)


if __name__ == '__main__':
    run_sync_daemon()
