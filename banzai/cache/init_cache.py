#!/usr/bin/env python
"""
Initialize the local calibration cache database and directory structure.
"""
import os
import argparse
from banzai import dbs
from banzai.logs import get_logger

logger = get_logger()


def init_cache(cache_db_address, cache_file_root, site_id):
    """
    Initialize cache database and directory structure.

    Parameters
    ----------
    cache_db_address : str
        SQLite database address (e.g., 'sqlite:////path/to/cache.db')
    cache_file_root : str
        Root directory for cached FITS files
    site_id : str
        Site code to cache (e.g., 'lsc', 'ogg')
    """
    # Create cache database with same schema as remote
    logger.info(f"Creating cache database at {cache_db_address}")
    dbs.create_db(cache_db_address)

    # Create directory structure for cached files
    logger.info(f"Creating cache directory structure at {cache_file_root}")
    cal_types = ['BIAS', 'DARK', 'SKYFLAT']
    for cal_type in cal_types:
        os.makedirs(os.path.join(cache_file_root, cal_type), exist_ok=True)

    logger.info("Cache initialization complete")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Initialize calibration cache')
    parser.add_argument('--cache-db', required=True,
                       help='SQLite database path (e.g., sqlite:////path/to/cache.db)')
    parser.add_argument('--cache-root', required=True,
                       help='Root directory for cached files')
    parser.add_argument('--site', required=True,
                       help='Site code (e.g., lsc, ogg)')

    args = parser.parse_args()
    init_cache(args.cache_db, args.cache_root, args.site)
