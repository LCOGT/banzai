"""
PostgreSQL Logical Replication Management

This module provides functions for managing PostgreSQL logical replication
for the BANZAI calibration cache system.
"""

from sqlalchemy import text, create_engine
from banzai import dbs
from banzai.logs import get_logger

logger = get_logger()


def setup_subscription(local_db_address, aws_connection_string, site_id,
                      publication_name='banzai_calibrations',
                      subscription_name=None, slot_name=None):
    """
    Create PostgreSQL logical replication subscription.

    This sets up a subscription from the local PostgreSQL database to the AWS
    source database, enabling automatic replication of calibration metadata.

    Parameters
    ----------
    local_db_address : str
        SQLAlchemy address for local PostgreSQL database
    aws_connection_string : str
        PostgreSQL connection string for AWS source database
        Format: 'host=aws-host.rds.amazonaws.com port=5432 dbname=calibrations
                 user=replication_user password=XXX sslmode=require'
    site_id : str
        Site identifier (e.g., 'lsc', 'ogg', 'cpt')
        Used to generate unique subscription and slot names
    publication_name : str, optional
        Name of publication on AWS database (default: 'banzai_calibrations')
    subscription_name : str, optional
        Name for the subscription (default: 'banzai_{site_id}_sub')
    slot_name : str, optional
        Replication slot name (default: 'banzai_{site_id}_slot')

    Raises
    ------
    Exception
        If subscription creation fails

    Notes
    -----
    - Each site must have a unique slot name to avoid conflicts
    - Inactive slots consume WAL space on AWS - must be dropped when sites are decommissioned
    - Subscription will perform initial table copy (copy_data=true)
    """
    # Auto-generate names if not provided
    if subscription_name is None:
        subscription_name = f'banzai_{site_id}_sub'
    if slot_name is None:
        slot_name = f'banzai_{site_id}_slot'

    logger.info(f"Setting up replication subscription: {subscription_name}")
    logger.info(f"  Publication: {publication_name}")
    logger.info(f"  Slot: {slot_name}")
    logger.info(f"  Site: {site_id}")

    # Build CREATE SUBSCRIPTION SQL
    # Note: We use single quotes for the connection string and publication name
    subscription_sql = f"""
    CREATE SUBSCRIPTION {subscription_name}
        CONNECTION '{aws_connection_string}'
        PUBLICATION {publication_name}
        WITH (
            copy_data = true,
            create_slot = true,
            slot_name = '{slot_name}',
            synchronous_commit = off
        );
    """

    try:
        # CREATE SUBSCRIPTION cannot run in a transaction block
        # Use raw connection with autocommit enabled
        engine = create_engine(local_db_address)
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(subscription_sql))
        logger.info(f"Successfully created subscription: {subscription_name}")
    except Exception as e:
        logger.error(f"Failed to create subscription: {e}")
        raise


def check_replication_health(db_address):
    """
    Check the health and status of PostgreSQL replication.

    Queries pg_stat_subscription to get replication lag, status, and other metrics.

    Parameters
    ----------
    db_address : str
        SQLAlchemy address for local PostgreSQL database

    Returns
    -------
    dict
        Dictionary with replication health metrics:
        - subname: Subscription name
        - pid: Worker process ID
        - received_lsn: Last received LSN
        - latest_end_lsn: Latest end LSN
        - last_msg_send_time: Last message send time
        - last_msg_receipt_time: Last message receipt time
        - latest_end_time: Latest end time
        - lag_seconds: Replication lag in seconds (None if not available)

    Returns empty dict if no subscription exists.

    Raises
    ------
    Exception
        If query fails
    """
    health_sql = """
    SELECT
        subname,
        pid,
        received_lsn,
        latest_end_lsn,
        last_msg_send_time,
        last_msg_receipt_time,
        latest_end_time,
        EXTRACT(EPOCH FROM (NOW() - last_msg_receipt_time)) as lag_seconds
    FROM pg_stat_subscription;
    """

    try:
        with dbs.get_session(db_address) as session:
            result = session.execute(text(health_sql)).fetchone()

        if result is None:
            logger.warning("No replication subscription found")
            return {}

        health = {
            'subname': result[0],
            'pid': result[1],
            'received_lsn': result[2],
            'latest_end_lsn': result[3],
            'last_msg_send_time': result[4],
            'last_msg_receipt_time': result[5],
            'latest_end_time': result[6],
            'lag_seconds': float(result[7]) if result[7] is not None else None
        }

        # Log status
        if health['lag_seconds'] is not None:
            logger.info(f"Replication lag: {health['lag_seconds']:.1f} seconds")
            if health['lag_seconds'] > 300:  # 5 minutes
                logger.warning(f"Replication lag is high: {health['lag_seconds']:.1f} seconds")
        else:
            logger.warning("Replication lag could not be determined")

        return health

    except Exception as e:
        logger.error(f"Failed to check replication health: {e}")
        raise


def drop_subscription(db_address, subscription_name, drop_slot=True):
    """
    Drop a PostgreSQL replication subscription.

    Use this when decommissioning a site or cleaning up failed subscriptions.

    Parameters
    ----------
    db_address : str
        SQLAlchemy address for local PostgreSQL database
    subscription_name : str
        Name of the subscription to drop
    drop_slot : bool, optional
        Whether to drop the replication slot on AWS (default: True)
        Set to False if you want to preserve the slot

    Raises
    ------
    Exception
        If subscription drop fails

    Notes
    -----
    IMPORTANT: If drop_slot=True, this will drop the replication slot on the
    AWS source database. If the slot is not dropped, it will continue to
    consume WAL space indefinitely.

    If the subscription was created with create_slot=true (which is our default),
    you should drop it with drop_slot=true to clean up the AWS slot.
    """
    logger.info(f"Dropping subscription: {subscription_name}")
    logger.info(f"  Drop slot: {drop_slot}")

    drop_sql = f"DROP SUBSCRIPTION IF EXISTS {subscription_name}"
    if drop_slot:
        drop_sql += " CASCADE"
    drop_sql += ";"

    try:
        # DROP SUBSCRIPTION cannot run in a transaction block
        # Use raw connection with autocommit enabled
        engine = create_engine(db_address)
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(drop_sql))
        logger.info(f"Successfully dropped subscription: {subscription_name}")
    except Exception as e:
        logger.error(f"Failed to drop subscription: {e}")
        raise


def get_subscription_status(db_address):
    """
    Get status of all subscriptions in the database.

    Parameters
    ----------
    db_address : str
        SQLAlchemy address for local PostgreSQL database

    Returns
    -------
    list of dict
        List of dictionaries with subscription information:
        - subname: Subscription name
        - subenabled: Whether subscription is enabled
        - subslotname: Replication slot name

    Raises
    ------
    Exception
        If query fails
    """
    status_sql = """
    SELECT
        subname,
        subenabled,
        subslotname
    FROM pg_subscription;
    """

    try:
        with dbs.get_session(db_address) as session:
            results = session.execute(text(status_sql)).fetchall()

        subscriptions = []
        for row in results:
            subscriptions.append({
                'subname': row[0],
                'subenabled': row[1],
                'subslotname': row[2]
            })

        return subscriptions

    except Exception as e:
        logger.error(f"Failed to get subscription status: {e}")
        raise
