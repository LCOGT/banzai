"""Smart stacking: per-camera polling worker + crash-only supervisor.

The worker is stateless: every few seconds it polls the ``stacks`` table for one camera and
does the obvious thing for each active stack (finalize when complete/timed out, otherwise
render an incremental preview). All lifecycle state — backoff timers, attempt counts, preview
progress — lives in the ``stacks`` row, never in process memory.
"""
import datetime
import multiprocessing
import multiprocessing.connection
import os
import sys
import time

from banzai import dbs, main, settings, smartstack_products
from banzai.context import Context
from banzai.logs import get_logger
from banzai.utils.messaging import post_to_shipper_queue

logger = get_logger()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REQUIRED_MESSAGE_FIELDS = ('fits_file', 'last_frame')

# Backoff between finalize attempts; after the ladder is exhausted the stack is marked 'error'.
FINALIZE_BACKOFF_SECONDS = [60, 300, 900, 3600]
MAX_FINALIZE_ATTEMPTS = len(FINALIZE_BACKOFF_SECONDS) + 1
CLEANUP_INTERVAL_SECONDS = 3600


def validate_message(body):
    """Check that body contains fits_file and last_frame."""
    return all(field in body for field in REQUIRED_MESSAGE_FIELDS)


def check_stack_complete(stackframes, frmtotal):
    """Return True if the stack is ready to finalize.

    Stackframe rows are written only after reduction succeeds, so a stack is complete when all
    expected rows are present or the instrument signalled is_last.
    """
    all_arrived = len(stackframes) == frmtotal
    has_last = any(stackframe.is_last for stackframe in stackframes)
    return bool(stackframes) and (all_arrived or has_last)


def stack_has_timed_out(stack_row, timeout_minutes, now=None):
    """Return True if no stackframe has arrived for longer than timeout_minutes.

    The clock is cadence-based: it resets on every arrival (``last_stackframe_at``), so a long
    total exposure never times out mid-stream — only a genuine gap in arrivals does.
    """
    if now is None:
        now = datetime.datetime.utcnow()
    return (now - stack_row.last_stackframe_at) > datetime.timedelta(minutes=timeout_minutes)


def finalize_stack(runtime_context, stack_row, stackframes, status):
    """Produce and ship the final e45 product for a terminal stack.

    The order is **claim -> write -> publish -> mark**, and each step is ordered deliberately:

    - **Claim first.** ``claim_finalize_attempt`` bumps the attempt counter and arms the backoff
      timer *before* any work runs, so a violent crash mid-finalize (OOM-kill, segfault) has
      already burned an attempt. That is what bounds a poison stack: after the backoff ladder it
      is marked ``error`` instead of crash-looping forever.
    - **Publish before mark.** The shipper is told about the products before the stack is recorded
      terminal, so a stack can never look complete without the shipper having been notified. A
      crash between publish and mark just leaves the stack active; the poll retries and re-ships.
      The shipper is idempotent on identical file paths, so at-least-once duplicates are harmless
      — a lost final product would not be.

    On any exception we only log. The claim already recorded the attempt, so there is nothing else
    to record; the poll retries once ``next_attempt_at`` has passed.
    """
    moluid = stack_row.moluid
    if stack_row.finalize_attempts >= MAX_FINALIZE_ATTEMPTS:
        dbs.mark_stack_terminal(runtime_context.db_address, moluid, 'error')
        logger.error('Smartstack exhausted finalize attempts; marking error',
                     extra_tags={'moluid': moluid, 'finalize_attempts': stack_row.finalize_attempts})
        return

    dbs.claim_finalize_attempt(runtime_context.db_address, moluid, FINALIZE_BACKOFF_SECONDS)
    try:
        fits_path, small_thumbnail, large_thumbnail = smartstack_products.run_final(
            stackframes, runtime_context, moluid)
        newest_stackframe = max(stackframes, key=lambda stackframe: stackframe.stack_num)
        post_to_shipper_queue(
            runtime_context.broker_url,
            runtime_context.SHIPPER_EXCHANGE,
            runtime_context.SHIPPER_QUEUE_NAME,
            fits_path=fits_path,
            small_thumbnail=small_thumbnail,
            large_thumbnail=large_thumbnail,
            instrument_enqueue_timestamp=newest_stackframe.instrument_enqueue_timestamp,
        )
        dbs.mark_stack_terminal(runtime_context.db_address, moluid, status)
        logger.info('Finalized smartstack', extra_tags={'moluid': moluid, 'status': status})
    except Exception:
        logger.error('Failed to finalize smartstack', exc_info=True, extra_tags={'moluid': moluid})


def process_camera_tick(runtime_context, camera):
    """Do one poll pass for a camera: finalize terminal stacks, otherwise preview growing ones."""
    now = datetime.datetime.utcnow()
    for stack in dbs.get_active_stacks(runtime_context.db_address, camera):
        stackframes = dbs.get_stackframes(runtime_context.db_address, stack.moluid)

        # Complete wins over timeout: a stack that has all its frames finalizes as 'complete'
        # even if it also happens to be past the cadence timeout.
        if check_stack_complete(stackframes, stack.frmtotal):
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
    """Poll one camera forever. Any exception is caught and logged so this loop cannot die from it.

    Only a violent death (OOM-kill, segfault) can end this process, and the supervisor relies on
    that: those are exactly the cases where a from-scratch restart is the right response.
    """
    runtime_context = Context(runtime_context_dict)
    logger.info('Starting stacking worker', extra_tags={'camera': camera})
    last_cleanup = 0.0
    while True:
        try:
            process_camera_tick(runtime_context, camera)
            # Retention is measured in days; scanning for expired stacks once an hour is plenty.
            if time.monotonic() - last_cleanup > CLEANUP_INTERVAL_SECONDS:
                dbs.cleanup_old_stacks(runtime_context.db_address, runtime_context.stack_retention_days)
                last_cleanup = time.monotonic()
        except Exception:
            logger.error('Error in stacking worker loop', exc_info=True, extra_tags={'camera': camera})
        time.sleep(poll_interval)


def run_supervisor():
    """Spawn one worker process per camera and let Docker handle every death — crash-only by design.

    Why this is a ~15-line supervisor and not a monitor-and-restart loop:

    - **Workers only die violently.** ``run_worker_loop`` catches every exception, so a child can
      only exit via OOM-kill or segfault. Those are precisely the failures where restarting from
      scratch is correct — and because workers are **stateless** (all lifecycle state lives in the
      ``stacks`` table), a restart loses nothing.
    - **Docker replaces the monitor.** ``restart: always`` on the compose service replaces ~80
      lines of health-check/restart machinery. A side benefit: every restart re-runs camera
      discovery, so a camera added after startup is picked up on the next restart.
    - **Claim-first attempts are what make this safe.** A poison stack (deterministic OOM on one
      stack's data) has already burned a finalize attempt when it kills the worker (see
      ``finalize_stack``). After the backoff ladder — a handful of container restarts over bounded
      minutes — the poll marks that stack ``error`` and all cameras resume permanently, with no
      operator intervention. Without claim-first, a poison stack would crash-loop forever under
      *any* process model.
    - **Accepted trade-off.** When one camera's worker dies violently, this process exits and Docker
      restarts the whole container, so every camera's worker briefly restarts too. That costs a few
      seconds of polling downtime and, because workers are stateless, nothing else.

    This function exits non-zero on any child death (and on an empty camera list) so ``restart:
    always`` brings the container back.
    """
    runtime_context = main.parse_args(
        settings,
        extra_console_arguments=[
            {'args': ['--site-id'],
             'kwargs': {'dest': 'site_id', 'required': True, 'help': 'Site identifier (e.g. lsc, ogg)'}},
            {'args': ['--stack-retention-days'],
             'kwargs': {'dest': 'stack_retention_days', 'type': int,
                        'default': int(os.getenv('STACK_RETENTION_DAYS', 30)),
                        'help': 'Days to retain terminal stacks before cleanup (default: 30)'}},
            {'args': ['--stack-timeout-minutes'],
             'kwargs': {'dest': 'stack_timeout_minutes', 'type': int,
                        'default': int(os.getenv('STACK_TIMEOUT_MINUTES', 20)),
                        'help': 'Minutes without a new stackframe before a partial stack is finalized (default: 20)'}},
            {'args': ['--instrument-types'],
             'kwargs': {'dest': 'instrument_types', 'default': os.getenv('INSTRUMENT_TYPES', '*'),
                        'help': 'Comma-separated instrument types to stack, or * for all (default: *)'}},
        ],
    )

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
