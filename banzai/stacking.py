"""Smart stacking: worker, supervisor, and helper functions."""
import argparse
import datetime
import multiprocessing
import signal
import time
from collections import defaultdict

import redis as redis_lib

from banzai import dbs
from banzai.logs import get_logger

logger = get_logger()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REQUIRED_MESSAGE_FIELDS = ('fits_file', 'last_frame', 'instrument_enqueue_timestamp')
INITIAL_FRAME_TIMEOUT_BUFFER_SECONDS = 60.0
INVALID_BASELINE_FALLBACK_SECONDS = 60.0
ADAPTIVE_TIMEOUT_MULTIPLIER = 2.0


def validate_message(body):
    """Check that body contains fits_file, last_frame, instrument_enqueue_timestamp."""
    return all(field in body for field in REQUIRED_MESSAGE_FIELDS)


def check_stack_complete(subframes, frmtotal):
    """Return True if the stack is ready to finalize.

    Subframe rows are written only after reduction succeeds, so a stack is
    complete when all expected rows are present or the instrument signalled
    is_last.
    """
    all_arrived = len(subframes) == frmtotal
    has_last = any(s.is_last for s in subframes)
    return bool(subframes) and (all_arrived or has_last)


def _arrival_ordered_subframes(subframes):
    return sorted(
        subframes,
        key=lambda s: (
            s.created_at or datetime.datetime.max,
            s.stack_num or 0,
        ),
    )


def _exptime_seconds(subframe):
    try:
        exptime = float(subframe.exptime or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return max(exptime, 0.0)


def adaptive_timeout_baseline_seconds(subframes):
    """Return the adjusted first-to-second-frame baseline or invalid-value fallback."""
    ordered_subframes = _arrival_ordered_subframes(subframes)
    if len(ordered_subframes) < 2:
        return None

    # The first two arrivals establish the expected post-exposure delivery cadence
    # for this moluid; later timeout checks compare against this baseline.
    exptime = _exptime_seconds(ordered_subframes[0])
    first_arrival = ordered_subframes[0].created_at
    second_arrival = ordered_subframes[1].created_at
    if first_arrival is None or second_arrival is None:
        return INVALID_BASELINE_FALLBACK_SECONDS

    # Subtract exposure duration because it is unrelated to processing/transport
    # delay, which is the signal this timeout is trying to detect.
    adjusted_gap = (second_arrival - first_arrival).total_seconds() - exptime
    # A zero or negative adjusted gap means the timing data is not usable, so
    # fall back to a conservative positive baseline instead of timing out instantly.
    if adjusted_gap <= 0:
        return INVALID_BASELINE_FALLBACK_SECONDS
    return adjusted_gap


def stack_has_timed_out(subframes, now=None):
    """Return True when a stack has exceeded the adaptive arrival timeout."""
    ordered_subframes = _arrival_ordered_subframes(subframes)
    if not ordered_subframes:
        return False

    now = now or datetime.datetime.utcnow()
    # Use the first frame's exposure time for the whole stack; smart-stack subframes
    # are expected to have consistent exposure durations.
    exptime = _exptime_seconds(ordered_subframes[0])
    first_arrival = ordered_subframes[0].created_at
    if first_arrival is None:
        return False

    # Until frame 2 arrives, there is no cadence measurement yet, so use the
    # first-frame exposure plus a fixed post-exposure buffer.
    if len(ordered_subframes) == 1:
        first_frame_timeout = exptime + INITIAL_FRAME_TIMEOUT_BUFFER_SECONDS
        return (now - first_arrival).total_seconds() > first_frame_timeout

    # After frame 2, the first-to-second adjusted gap defines the expected
    # post-exposure arrival cadence; future gaps get twice that allowance.
    baseline = adaptive_timeout_baseline_seconds(ordered_subframes)
    timeout_threshold = baseline * ADAPTIVE_TIMEOUT_MULTIPLIER

    # Late frames should still cause a timeout even if they eventually arrived;
    # the arrival timestamps preserve those delayed adjacent gaps.
    for previous_frame, current_frame in zip(ordered_subframes, ordered_subframes[1:]):
        if previous_frame.created_at is None or current_frame.created_at is None:
            continue
        adjusted_gap = (current_frame.created_at - previous_frame.created_at).total_seconds() - exptime
        if adjusted_gap > timeout_threshold:
            return True

    # If no already-observed gap was too long, check whether the next expected
    # frame has now been missing for too long.
    latest_arrival = ordered_subframes[-1].created_at
    if latest_arrival is None:
        return False
    missing_frame_gap = (now - latest_arrival).total_seconds() - exptime
    return missing_frame_gap > timeout_threshold


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

def run_worker_loop(camera, db_address, redis_url, retention_days=30, poll_interval=5):
    """Main loop: drain notifications, query DB, check completion, finalize."""
    redis_client = redis_lib.Redis.from_url(redis_url)
    while True:
        try:
            process_notifications(db_address, redis_client, camera)
            check_timeouts(db_address, camera)
            dbs.cleanup_old_subframes(db_address, retention_days)
            time.sleep(poll_interval)
        except Exception as e:
            logger.error(f'Error in stacking worker loop: {e}', extra_tags={'camera': camera})
            time.sleep(poll_interval)


def process_notifications(db_address, redis_client, camera):
    """Drain, deduplicate, and process latest state for each moluid."""
    moluids = drain_notifications(redis_client, camera)
    for moluid in moluids:
        subframes = dbs.get_subframes(db_address, moluid)
        if not subframes:
            continue
        frmtotal = subframes[0].frmtotal
        if check_stack_complete(subframes, frmtotal):
            finalize_stack(db_address, moluid, status='complete')


def finalize_stack(db_address, moluid, status='complete'):
    """Mark stack complete and log mock stacking/JPEG/ingester operations."""
    dbs.mark_stack_complete(db_address, moluid, status=status)
    logger.debug(f'Mock stacking complete for {moluid}', extra_tags={'moluid': moluid})
    logger.debug(f'Mock JPEG generation for {moluid}', extra_tags={'moluid': moluid})
    logger.debug(f'Mock ingester upload for {moluid}', extra_tags={'moluid': moluid})


def check_timeouts(db_address, camera, now=None):
    """Finalize active stacks that are complete or have exceeded adaptive timeout."""
    active_subframes = dbs.get_active_subframes_for_camera(db_address, camera)
    # The DB scan is camera-wide, so regroup rows into independent stack states
    # before making terminal-status decisions.
    subframes_by_moluid = defaultdict(list)
    for subframe in active_subframes:
        subframes_by_moluid[subframe.moluid].append(subframe)

    for moluid, subframes in subframes_by_moluid.items():
        frmtotal = subframes[0].frmtotal
        # Completion wins over timeout so a fully reduced stack cannot be marked
        # partial just because its cadence also exceeded the adaptive threshold.
        if check_stack_complete(subframes, frmtotal):
            finalize_stack(db_address, moluid, status='complete')
        # Incomplete active stacks are finalized only when their arrival cadence
        # indicates that the next expected frame is no longer arriving normally.
        elif stack_has_timed_out(subframes, now=now):
            finalize_stack(db_address, moluid, status='timeout')


# ---------------------------------------------------------------------------
# Supervisor
# ---------------------------------------------------------------------------

class StackingSupervisor:
    def __init__(self, site_id, db_address, redis_url, retention_days=30):
        self.site_id = site_id
        self.db_address = db_address
        self.redis_url = redis_url
        self.retention_days = retention_days
        self.workers = {}

    def _worker_args(self, camera):
        return (camera, self.db_address, self.redis_url, self.retention_days)

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


def _stacking_worker_arg_parser():
    parser = argparse.ArgumentParser(description='Run the smart stacking supervisor.')
    parser.add_argument('--site-id', dest='site_id', required=True,
                        help='Site identifier (e.g. lsc, ogg)')
    parser.add_argument('--db-address', dest='db_address', required=True,
                        help='Database connection string')
    parser.add_argument('--redis-url', dest='redis_url', required=True,
                        help='Redis URL')
    parser.add_argument('--stack-retention-days', dest='stack_retention_days', type=int, default=30,
                        help='Days to retain completed stacks (default: 30)')
    return parser


def run_supervisor():
    """Entry point for the stacking supervisor."""
    args = _stacking_worker_arg_parser().parse_args()

    supervisor = StackingSupervisor(args.site_id, args.db_address, args.redis_url,
                                    retention_days=args.stack_retention_days)

    def handle_signal(signum, frame):
        supervisor.shutdown()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    supervisor.start()
    supervisor.monitor()
