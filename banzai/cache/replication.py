from psycopg2 import errors as pg_errors
from sqlalchemy import text, create_engine
from sqlalchemy.exc import OperationalError

from banzai.logs import get_logger

# PostgreSQL logical replication is managed via server-level DDL commands
# (CREATE/DROP SUBSCRIPTION) that cannot run inside transaction blocks and
# have no SQLAlchemy ORM representation. Raw SQL with AUTOCOMMIT is required.
logger = get_logger()


def _subscription_sql(subscription_name, aws_connection_string, publication_name,
                      slot_name, create_slot=True):
    return f"""
    CREATE SUBSCRIPTION {subscription_name}
        CONNECTION '{aws_connection_string}'
        PUBLICATION {publication_name}
        WITH (
            copy_data = true,
            create_slot = {'true' if create_slot else 'false'},
            slot_name = '{slot_name}',
            synchronous_commit = off
        );
    """


def setup_subscription(local_db_address, aws_connection_string, site_id,
                      publication_name='banzai_calibrations',
                      subscription_name=None, slot_name=None):
    # aws_connection_string uses libpq format: 'host=... port=5432 dbname=... user=... password=...'
    # Inactive slots consume WAL space on AWS — must be dropped when sites are decommissioned.
    if subscription_name is None:
        subscription_name = f'banzai_{site_id}_sub'
    if slot_name is None:
        slot_name = f'banzai_{site_id}_slot'

    logger.info("Setting up replication subscription",
                extra_tags={'subscription': subscription_name, 'publication': publication_name,
                            'slot': slot_name, 'site': site_id})

    engine = create_engine(local_db_address)
    try:
        sql = _subscription_sql(subscription_name, aws_connection_string,
                                publication_name, slot_name, create_slot=True)
        # CREATE SUBSCRIPTION cannot run inside a transaction block
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute(text(sql))
    except OperationalError as e:
        if isinstance(e.orig, pg_errors.ProtocolViolation) and 'already exists' in str(e.orig):
            logger.info(f"Replication slot '{slot_name}' already exists on publisher, reusing it")
            sql = _subscription_sql(subscription_name, aws_connection_string,
                                    publication_name, slot_name, create_slot=False)
            with engine.connect() as conn:
                conn = conn.execution_options(isolation_level="AUTOCOMMIT")
                conn.execute(text(sql))
        else:
            raise
    finally:
        engine.dispose()

    logger.info(f"Successfully created subscription: {subscription_name}")


def drop_subscription(db_address, subscription_name):
    # Also drops the replication slot on AWS.
    # Undropped slots consume WAL space indefinitely.
    logger.info(f"Dropping subscription: {subscription_name}")

    drop_sql = f"DROP SUBSCRIPTION IF EXISTS {subscription_name};"

    try:
        # DROP SUBSCRIPTION cannot run inside a transaction block
        engine = create_engine(db_address)
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute(text(drop_sql))
        engine.dispose()
        logger.info(f"Successfully dropped subscription: {subscription_name}")
    except Exception as e:
        logger.error(f"Failed to drop subscription: {e}")
        raise
