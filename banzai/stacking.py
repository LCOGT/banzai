"""Smartstack polling workers and their supervisor.

Each camera has a worker that polls active stacks, renders previews as frames arrive, and
finalizes stacks when they complete or time out. Progress and retry state are stored in the
database so a restarted worker can continue where the previous process stopped.
"""
import datetime
import multiprocessing
import multiprocessing.connection
import os
import sys
import time

from banzai import dbs, smartstack_products
from banzai.context import Context
from banzai.logs import get_logger
from banzai.utils.messaging import post_to_shipper_queue

logger = get_logger()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REQUIRED_MESSAGE_FIELDS = ('fits_file', 'last_frame')

# Backoff between finalize attempts; after the ladder is exhausted the stack is marked 'error'.
# Read from the environment (comma-separated seconds) so deployments — notably the site e2e
# suite — can shorten the ladder without changing the default production cadence.
_backoff_env = os.getenv('FINALIZE_BACKOFF_SECONDS', '60,300,900,3600')
FINALIZE_BACKOFF_SECONDS = [int(seconds) for seconds in _backoff_env.split(',') if seconds.strip()]
if not FINALIZE_BACKOFF_SECONDS:
    raise ValueError(f'FINALIZE_BACKOFF_SECONDS must contain at least one integer, got {_backoff_env!r}')
MAX_FINALIZE_ATTEMPTS = len(FINALIZE_BACKOFF_SECONDS) + 1
CLEANUP_INTERVAL_SECONDS = 3600


def validate_message(body):
    """Check that body contains fits_file and last_frame."""
    return all(field in body for field in REQUIRED_MESSAGE_FIELDS)


def check_stack_complete(stackframes):
    """Return True when the instrument signalled the final stackframe."""
    return any(stackframe.is_last for stackframe in stackframes)


def stack_has_timed_out(stack_row, timeout_minutes, now=None):
    """Return True when the time since the most recent stackframe exceeds timeout_minutes.

    ``last_stackframe_at`` is updated whenever a reduced stackframe arrives.
    """
    if now is None:
        now = datetime.datetime.utcnow()
    return (now - stack_row.last_stackframe_at) > datetime.timedelta(minutes=timeout_minutes)


def finalize_stack(runtime_context, stack_row, stackframes, status):
    """Create and publish final products for a complete or timed-out stack.

    Claim the attempt before generating products so later failures count toward the retry limit.
    Publish the products before marking the stack terminal so terminal state is recorded only
    after the shipper has been notified. If generation or publishing fails, leave the stack active
    and retry after ``next_attempt_at``. Mark the stack as error when its attempt budget is exhausted.
    """
    moluid = stack_row.moluid
    if stack_row.finalize_attempts >= MAX_FINALIZE_ATTEMPTS:
        dbs.mark_stack_terminal(runtime_context.db_address, moluid, 'error')
        logger.error('Smartstack exhausted finalize attempts; marking error',
                     extra_tags={'smartstack_event': 'terminal',
                                 'smartstack_status': 'error',
                                 'smartstack_moluid': moluid,
                                 'smartstack_camera': stack_row.camera,
                                 'smartstack_finalize_attempt': stack_row.finalize_attempts})
        return

    finalize_attempt = dbs.claim_finalize_attempt(runtime_context.db_address, moluid, FINALIZE_BACKOFF_SECONDS)
    try:
        fits_path, small_thumbnail, large_thumbnail = smartstack_products.run_final(
            stackframes, runtime_context, moluid)
        post_to_shipper_queue(
            runtime_context.broker_url,
            runtime_context.SHIPPER_EXCHANGE,
            runtime_context.SHIPPER_QUEUE_NAME,
            fits_path=fits_path,
            small_thumbnail=small_thumbnail,
            large_thumbnail=large_thumbnail,
        )
        dbs.mark_stack_terminal(runtime_context.db_address, moluid, status)
        logger.info('Finalized smartstack', extra_tags={'smartstack_event': 'terminal',
                                                        'smartstack_status': status,
                                                        'smartstack_moluid': moluid,
                                                        'smartstack_camera': stack_row.camera,
                                                        'smartstack_finalize_attempt': finalize_attempt})
    except Exception:
        logger.error('Failed to finalize smartstack', exc_info=True,
                     extra_tags={'smartstack_event': 'finalize_failed',
                                 'smartstack_target_status': status,
                                 'smartstack_moluid': moluid,
                                 'smartstack_camera': stack_row.camera,
                                 'smartstack_finalize_attempt': finalize_attempt})


def process_camera_tick(runtime_context, camera):
    """Do one poll pass for a camera: finalize terminal stacks, otherwise preview growing ones."""
    now = datetime.datetime.utcnow()
    for stack in dbs.get_active_stacks(runtime_context.db_address, camera):
        stackframes = dbs.get_stackframes(runtime_context.db_address, stack.moluid)

        # Complete wins over timeout: a stack with the final-frame signal finalizes as
        # 'complete' even if it also happens to be past the cadence timeout.
        if check_stack_complete(stackframes):
            terminal_status = 'complete'
        elif stack_has_timed_out(stack, runtime_context.stack_timeout_minutes, now=now):
            terminal_status = 'timeout'
        else:
            terminal_status = None

        if terminal_status is not None:
            if stack.next_attempt_at is not None and now < stack.next_attempt_at:
                continue  # Still inside the backoff window from a previous failed attempt.
            finalize_stack(runtime_context, stack, stackframes, terminal_status)
        # Reductions finish concurrently; wait for frame 1 so every preview reuses its basename.
        elif (runtime_context.SMARTSTACK_PREVIEWS
              and stackframes
              and stackframes[0].stack_num == 1
              and len(stackframes) > stack.last_preview_count):
            try:
                smartstack_products.run_preview(stackframes, runtime_context, stack.moluid)
                dbs.set_preview_count(runtime_context.db_address, stack.moluid, len(stackframes))
            except Exception:
                # Do not advance the preview count on failure so the next tick retries it.
                logger.warning('Failed to render smartstack preview', exc_info=True,
                               extra_tags={'moluid': stack.moluid})


def run_worker_loop(camera, runtime_context_dict, poll_interval=5):
    """Continuously process active stacks for one camera.

    Each pass finalizes or previews eligible stacks and periodically removes expired stack records.
    Errors during a pass are logged, then the worker waits ``poll_interval`` seconds before trying
    again.
    """
    runtime_context = Context(runtime_context_dict)
    logger.info('Starting stacking worker', extra_tags={'camera': camera})
    last_cleanup = time.monotonic() - CLEANUP_INTERVAL_SECONDS
    while True:
        try:
            process_camera_tick(runtime_context, camera)
            # Retention is configured in days, so run cleanup hourly instead of on every poll.
            if time.monotonic() - last_cleanup > CLEANUP_INTERVAL_SECONDS:
                dbs.cleanup_old_stacks(runtime_context.db_address, runtime_context.stack_retention_days)
                last_cleanup = time.monotonic()
        except Exception:
            logger.error('Error in stacking worker loop', exc_info=True, extra_tags={'camera': camera})
        time.sleep(poll_interval)


def run_supervisor(runtime_context):
    """Start one polling worker for each selected camera.

    This design keeps supervision simple by relying on the container restart policy for worker
    lifecycle management. The supervisor starts all workers and waits for one to exit. It then
    exits with status 1 so the container can restart, rediscover cameras, and start a new set of
    workers. The same restart path is used when no cameras match the configured filters.

    Restarting the container also restarts workers that were still healthy. This is the trade-off
    for avoiding a separate per-worker restart loop. Stack progress is stored in the database, so
    replacement workers continue from the persisted state.

    Before generating final products, ``finalize_stack`` records the attempt and its backoff. If a
    worker exits after that claim, the replacement process retains the attempt count and waits
    until ``next_attempt_at`` before retrying. Once the attempt limit is reached, the stack is
    marked ``error`` instead of being finalized again.
    """
    instrument_types = ([t.strip() for t in runtime_context.instrument_types.split(',')]
                        if runtime_context.instrument_types != '*' else ['*'])
    instruments = dbs.get_instruments_at_site(runtime_context.site_id, runtime_context.db_address)
    if instrument_types != ['*']:
        instruments = [instrument for instrument in instruments if instrument.type in instrument_types]
    cameras = [instrument.camera for instrument in instruments]
    if not cameras:
        logger.error('No cameras found at site; exiting so Docker restarts the container',
                     extra_tags={'site_id': runtime_context.site_id})
        sys.exit(1)

    runtime_context_dict = vars(runtime_context)
    processes = [multiprocessing.Process(target=run_worker_loop, args=(camera, runtime_context_dict), daemon=True)
                 for camera in cameras]
    for process in processes:
        process.start()
    logger.info('Started stacking workers',
                extra_tags={'site_id': runtime_context.site_id, 'camera_count': len(processes)})

    multiprocessing.connection.wait([process.sentinel for process in processes])
    logger.error('A stacking worker exited; exiting so Docker restarts the container')
    sys.exit(1)
