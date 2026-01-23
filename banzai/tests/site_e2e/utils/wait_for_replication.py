"""
Utilities for waiting on PostgreSQL replication sync.
"""
import logging
import time

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)


def wait_for_replication_sync(db_address: str, timeout: int = 60) -> bool:
    """
    Wait for replication subscription to be synced.

    Queries pg_stat_subscription to check if subscription is active and synced.

    Args:
        db_address: PostgreSQL connection string for the subscriber database
        timeout: Maximum seconds to wait for sync

    Returns:
        True if synced within timeout, False otherwise.
    """
    engine = create_engine(db_address)
    start_time = time.time()
    poll_interval = 2

    logger.info("Waiting for replication subscription to sync...")

    while time.time() - start_time < timeout:
        try:
            with engine.connect() as conn:
                # Check pg_subscription for enabled subscriptions, then verify
                # they're active in pg_stat_subscription (have a worker PID)
                result = conn.execute(text("""
                    SELECT s.subname, s.subenabled, ss.pid
                    FROM pg_subscription s
                    LEFT JOIN pg_stat_subscription ss ON s.subname = ss.subname
                    WHERE s.subenabled = true
                """))
                rows = result.fetchall()

                if rows:
                    active = [row for row in rows if row.pid is not None]
                    if active:
                        for row in active:
                            logger.info(f"Subscription '{row.subname}' is enabled and active (pid={row.pid})")
                        return True
                    else:
                        elapsed = int(time.time() - start_time)
                        logger.info(f"Subscriptions exist but no worker active yet... ({elapsed}s elapsed)")
                else:
                    elapsed = int(time.time() - start_time)
                    logger.info(f"No enabled subscriptions yet, waiting... ({elapsed}s elapsed)")

        except Exception as e:
            elapsed = int(time.time() - start_time)
            logger.warning(f"Error checking subscription status: {e} ({elapsed}s elapsed)")

        time.sleep(poll_interval)

    logger.error(f"Timeout waiting for replication sync after {timeout} seconds")
    return False
