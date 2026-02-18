from sqlalchemy import text, create_engine
from banzai import dbs
from banzai.logs import get_logger

logger = get_logger()


def setup_subscription(local_db_address, aws_connection_string, site_id,
                      publication_name='banzai_calibrations',
                      subscription_name=None, slot_name=None):
    # aws_connection_string uses libpq format: 'host=... port=5432 dbname=... user=... password=...'
    # Inactive slots consume WAL space on AWS — must be dropped when sites are decommissioned.
    if subscription_name is None:
        subscription_name = f'banzai_{site_id}_sub'
    if slot_name is None:
        slot_name = f'banzai_{site_id}_slot'

    logger.info(f"Setting up replication subscription: {subscription_name}")
    logger.info(f"  Publication: {publication_name}")
    logger.info(f"  Slot: {slot_name}")
    logger.info(f"  Site: {site_id}")

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
        # CREATE SUBSCRIPTION cannot run inside a transaction block
        engine = create_engine(local_db_address)
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(subscription_sql))
        logger.info(f"Successfully created subscription: {subscription_name}")
    except Exception as e:
        logger.error(f"Failed to create subscription: {e}")
        raise


def check_replication_health(db_address):
    """Returns dict of replication metrics from pg_stat_subscription, or {} if no subscription exists."""
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

        if health['lag_seconds'] is not None:
            logger.info(f"Replication lag: {health['lag_seconds']:.1f} seconds")
            if health['lag_seconds'] > 300:
                logger.warning(f"Replication lag is high: {health['lag_seconds']:.1f} seconds")
        else:
            logger.warning("Replication lag could not be determined")

        return health

    except Exception as e:
        logger.error(f"Failed to check replication health: {e}")
        raise


def drop_subscription(db_address, subscription_name, drop_slot=True):
    # If drop_slot=True, this also drops the replication slot on AWS.
    # Undropped slots consume WAL space indefinitely.
    logger.info(f"Dropping subscription: {subscription_name}")
    logger.info(f"  Drop slot: {drop_slot}")

    drop_sql = f"DROP SUBSCRIPTION IF EXISTS {subscription_name}"
    if drop_slot:
        drop_sql += " CASCADE"
    drop_sql += ";"

    try:
        # DROP SUBSCRIPTION cannot run inside a transaction block
        engine = create_engine(db_address)
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(drop_sql))
        logger.info(f"Successfully dropped subscription: {subscription_name}")
    except Exception as e:
        logger.error(f"Failed to drop subscription: {e}")
        raise
