"""Smart stacking: worker, supervisor, and helper functions."""
import argparse
import datetime
import multiprocessing
import signal
import time

import redis as redis_lib

from banzai import dbs
from banzai.logs import get_logger

logger = get_logger()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REQUIRED_MESSAGE_FIELDS = ('fits_file', 'last_frame', 'instrument_enqueue_timestamp')


def validate_message(body):
    """Check that body contains fits_file, last_frame, instrument_enqueue_timestamp."""
    return all(field in body for field in REQUIRED_MESSAGE_FIELDS)


def check_stack_complete(frames, frmtotal):
    """Return True if the stack is ready to finalize.

    A stack is complete when all received frames have been reduced and either
    all expected frames are present or the instrument signalled is_last.
    """
    all_reduced = all(f.filepath is not None for f in frames)
    all_arrived = len(frames) == frmtotal
    has_last = any(f.is_last for f in frames)
    return all_reduced and (all_arrived or has_last)


# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------

REDIS_KEY_PREFIX = 'stack:notify:'


def push_notification(redis_client, camera, moluid):
    """Push a moluid notification onto the Redis list for a camera."""
    redis_client.lpush(f'{REDIS_KEY_PREFIX}{camera}', moluid)


def drain_notifications(redis_client, camera):
    """Drain and return a deduplicated set of moluids from the Redis list for a camera."""
    key = f'{REDIS_KEY_PREFIX}{camera}'
    drain_key = f'{key}:draining'
    # Atomic rename so notifications pushed between read and delete aren't lost
    try:
        redis_client.rename(key, drain_key)
    except redis_lib.exceptions.ResponseError:
        return set()
    raw = redis_client.lrange(drain_key, 0, -1)
    redis_client.delete(drain_key)
    return {item.decode() if isinstance(item, bytes) else item for item in raw}


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

def run_worker_loop(camera, db_address, redis_url, timeout_minutes=20, retention_days=30, poll_interval=5):
    """Main loop: drain notifications, query DB, check completion, finalize."""
    redis_client = redis_lib.Redis.from_url(redis_url)
    while True:
        process_notifications(db_address, redis_client, camera)
        check_timeout(db_address, camera, timeout_minutes)
        dbs.cleanup_old_records(db_address, retention_days)
        time.sleep(poll_interval)


def process_notifications(db_address, redis_client, camera):
    """Drain, deduplicate, and process latest state for each moluid."""
    moluids = drain_notifications(redis_client, camera)
    for moluid in moluids:
        frames = dbs.get_stack_frames(db_address, moluid)
        if not frames:
            continue
        frmtotal = frames[0].frmtotal
        if check_stack_complete(frames, frmtotal):
            finalize_stack(db_address, moluid, status='complete')


def finalize_stack(db_address, moluid, status='complete'):
    """Mark stack complete and log mock stacking/JPEG/ingester operations."""
    dbs.mark_stack_complete(db_address, moluid, status=status)
    logger.info(f'Mock stacking complete for {moluid}', extra_tags={'moluid': moluid})
    logger.info(f'Mock JPEG generation for {moluid}', extra_tags={'moluid': moluid})
    logger.info(f'Mock ingester upload for {moluid}', extra_tags={'moluid': moluid})


def check_timeout(db_address, camera, timeout_minutes):
    """Find stale active stacks and finalize them with status='timeout'."""
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(minutes=timeout_minutes)
    with dbs.get_session(db_address) as session:
        stale_moluids = session.query(dbs.StackFrame.moluid).filter(
            dbs.StackFrame.camera == camera,
            dbs.StackFrame.status == 'active',
            dbs.StackFrame.dateobs < cutoff,
        ).distinct().all()
    for (moluid,) in stale_moluids:
        finalize_stack(db_address, moluid, status='timeout')


# ---------------------------------------------------------------------------
# Supervisor
# ---------------------------------------------------------------------------

class StackingSupervisor:
    def __init__(self, site_id, db_address, redis_url, timeout_minutes=20, retention_days=30):
        self.site_id = site_id
        self.db_address = db_address
        self.redis_url = redis_url
        self.timeout_minutes = timeout_minutes
        self.retention_days = retention_days
        self.workers = {}

    def _worker_args(self, camera):
        return (camera, self.db_address, self.redis_url, self.timeout_minutes, self.retention_days)

    def start(self):
        """Discover cameras and spawn one worker process per camera."""
        cameras = [inst.camera for inst in dbs.get_instruments_at_site(self.site_id, self.db_address)]
        for camera in cameras:
            proc = multiprocessing.Process(
                target=run_worker_loop,
                args=self._worker_args(camera),
                name=f'stacking-worker-{camera}',
            )
            proc.start()
            self.workers[camera] = proc
            logger.info(f'Started stacking worker for camera {camera}')

    def monitor(self, check_interval=10):
        """Check worker health and restart crashed workers."""
        while True:
            for camera, proc in list(self.workers.items()):
                if not proc.is_alive():
                    logger.warning(f'Worker for {camera} died, restarting')
                    new_proc = multiprocessing.Process(
                        target=run_worker_loop,
                        args=self._worker_args(camera),
                        name=f'stacking-worker-{camera}',
                    )
                    new_proc.start()
                    self.workers[camera] = new_proc
            time.sleep(check_interval)

    def shutdown(self):
        """Graceful shutdown of all workers."""
        for camera, proc in self.workers.items():
            proc.terminate()
            proc.join(timeout=10)
            logger.info(f'Stopped stacking worker for camera {camera}')
        self.workers.clear()


def create_parser():
    parser = argparse.ArgumentParser(description='Run the smart stacking supervisor.')
    parser.add_argument('--site-id', dest='site_id', required=True,
                        help='Site identifier (e.g. lsc, ogg)')
    parser.add_argument('--db-address', dest='db_address', required=True,
                        help='Database connection string')
    parser.add_argument('--redis-url', dest='redis_url', default='redis://redis:6379/0',
                        help='Redis URL (default: redis://redis:6379/0)')
    parser.add_argument('--stack-timeout-minutes', dest='stack_timeout_minutes', type=int, default=20,
                        help='Minutes before an incomplete stack times out (default: 20)')
    parser.add_argument('--stack-retention-days', dest='stack_retention_days', type=int, default=30,
                        help='Days to retain completed stacks (default: 30)')
    return parser


def run_supervisor():
    """Entry point for the stacking supervisor."""
    args = create_parser().parse_args()

    supervisor = StackingSupervisor(args.site_id, args.db_address, args.redis_url,
                                    timeout_minutes=args.stack_timeout_minutes,
                                    retention_days=args.stack_retention_days)

    def handle_signal(signum, frame):
        supervisor.shutdown()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    supervisor.start()
    supervisor.monitor()
