"""
Cleanup utilities for site E2E tests.

Provides functions to clean up Docker containers, test data, and database
replication subscriptions after test runs.
"""

import os
import shutil
import subprocess

from sqlalchemy import create_engine, text

from banzai.logs import get_logger

logger = get_logger()


def cleanup_docker(compose_files: list[str], env_file: str = None):
    """
    Stop and remove containers/volumes for the given compose files.

    Parameters
    ----------
    compose_files : list[str]
        List of Docker Compose file paths
    env_file : str, optional
        Path to environment file to use with docker compose
    """
    for compose_file in compose_files:
        logger.info(f"Cleaning up Docker containers from: {compose_file}")
        try:
            cmd = ["docker", "compose", "-f", compose_file]
            if env_file:
                cmd.extend(["--env-file", env_file])
            cmd.extend(["down", "-v"])
            subprocess.run(cmd, capture_output=True, text=True)
            logger.info(f"  Cleaned up: {compose_file}")
        except Exception as e:
            logger.warning(f"  Failed to cleanup {compose_file}: {e}")


def cleanup_data(data_dir: str):
    """
    Remove temporary test data directories.

    Parameters
    ----------
    data_dir : str
        Base directory containing test data
    """
    subdirs = [
        "raw",
        "calibrations",
        "output",
        "postgres-publication",
        "postgres-local",
    ]

    for subdir in subdirs:
        path = os.path.join(data_dir, subdir)
        if os.path.exists(path):
            logger.info(f"Removing test data directory: {path}")
            shutil.rmtree(path, ignore_errors=True)


def drop_replication_subscription(db_address: str, site_id: str):
    """
    Drop the replication subscription to release the slot on publication DB.

    Parameters
    ----------
    db_address : str
        SQLAlchemy database connection string for the local database
    site_id : str
        Site identifier used in subscription name (e.g., 'lsc', 'ogg')
    """
    subscription_name = f"banzai_{site_id}_sub"
    logger.info(f"Dropping replication subscription: {subscription_name}")

    try:
        # DROP SUBSCRIPTION cannot run in a transaction block
        engine = create_engine(db_address)
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(f"DROP SUBSCRIPTION IF EXISTS {subscription_name}"))
        logger.info(f"  Dropped subscription: {subscription_name}")
    except Exception as e:
        logger.warning(f"  Failed to drop subscription {subscription_name}: {e}")
