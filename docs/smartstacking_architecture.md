# Smart Stacking Architecture

**Status:** in progress on `feature/separate-site-deps`
**Scope:** subframe ingestion → reduction → per-camera coordination → finalized stack

This document describes the process layout and message flow for the
smart-stacking subsystem deployed at each observatory site.
It aims to give a clearer picture of *who runs what*, *how completion is
signalled*, and *why the current implementation uses RabbitMQ plus two Redis
roles*.

---

## 1. Process layout

Three long-running services cooperate at each site, plus the shared
infrastructure they depend on. Boxes are OS processes or process groups.
Arrows show the main runtime relationships and state changes.

```mermaid
flowchart LR
    subgraph site["Site deployment"]
        direction LR
        listener["banzai-subframe-listener
          (SubframeListener)"]
        celery["banzai-subframe-worker
          (Celery worker pool)"]
        supervisor["banzai-stacking-supervisor
          (StackingSupervisor)"]
        wA["stacking-worker-camA
          (run_worker_loop)"]
        wB["stacking-worker-camB
          (run_worker_loop)"]
        wN["stacking-worker-camN"]
        supervisor -- "spawns and monitors" --> wA
        supervisor -- "spawns and monitors" --> wB
        supervisor -- "spawns and monitors" --> wN
    end

    rabbit[("RabbitMQ
    STACK_QUEUE_NAME")]
    celery_broker[("Redis Celery broker
      TASK_HOST")]
    notify_redis[("Redis
      stack:notify:{camera}")]
    db[("PostgreSQL
      subframes table")]
    reduced["Reduced FITS files"]

    rabbit -- "consumes" --> listener
    listener -- "enqueue process_subframe" --> celery_broker
    celery_broker -- "delivers task" --> celery
    celery -- "run reduction pipeline" --> reduced
    celery -- "insert reduced row" --> db
    celery -- "subframe moluid" --> notify_redis
    wA -- "poll, drain" --> notify_redis
    wB -- "poll, drain" --> notify_redis
    wA -- "get state and update stack status" --> db
    wB -- "get state and update stack status" --> db
```

**Process responsibilities at a glance:**

| Process | Entry point | Role |
|---------|-------------|------|
| `banzai-subframe-listener` | `banzai.main:run_subframe_worker` | RabbitMQ consumer; validates payload, dispatches Celery task. |
| `banzai-subframe-worker` | Celery worker on `SUBFRAME_TASK_QUEUE_NAME` | Runs `process_subframe`: insert row, reduce frame, update row, notify stack workers through Redis. |
| `banzai-stacking-supervisor` | `banzai.stacking:run_supervisor` | Discovers cameras at site; spawns and monitors one worker per camera. |
| `stacking-worker-{camera}` | `banzai.stacking:run_worker_loop` (`multiprocessing` child) | Coalesces per-camera notifications and decides when a stack is complete. |

Cameras are discovered once at supervisor startup from
`dbs.get_instruments_at_site(site_id)`. Each per-camera worker is a separate OS
process so a crash in one camera's loop doesn't affect the others; the
supervisor restarts crashed workers.

---

## 2. Subframe → stack sequence

This is what happens when a single subframe arrives and ultimately triggers
stack finalization. The inbound subframe
payload is consumed from RabbitMQ, the reduction task is delivered by Celery
through `TASK_HOST`, and notifications of new subframes being ready go
through a Redis list.

```mermaid
sequenceDiagram
    autonumber
    participant Inst as Instrument producer
    participant MQ as RabbitMQ STACK_QUEUE_NAME
    participant L as SubframeListener
    participant CB as Redis Celery broker
    participant W as process_subframe worker
    participant DB as Postgres subframes
    participant NR as Redis stack notify camera
    participant S as Per-camera stack worker

    Inst->>MQ: publish JSON payload
    MQ->>L: deliver message
    L->>L: validate_message(body)
    L->>CB: enqueue process_subframe task
    L-->>MQ: ack

    CB->>W: deliver Celery task
    activate W
    W->>W: run_pipeline_stages
    W->>DB: INSERT Subframe with reduced filepath
    W->>NR: LPUSH moluid notification
    deactivate W

    loop stack worker tick
        S->>NR: poll and drain notifications for camera
        NR-->>S: deduplicated moluid set
        S->>DB: load subframes for each moluid
        DB-->>S: current durable state
        alt stack is complete
            S->>DB: mark_stack_complete(status complete)
            S->>S: finalize stack products
        else stack is still incomplete
            S->>S: leave active until another tick
        end
    end

```

- The DB row is inserted **after** reduction succeeds, so every new
  `subframes.filepath` value points at a reduced file.
- The Redis notification is pushed **after** the reduced row is inserted, so
  when the stacking worker drains and queries, it observes the ready-to-stack
  state.
- `drain_notifications` uses `RENAME` + `LRANGE` + `DEL` to atomically swap
  the list, so notifications pushed during a drain aren't lost.
- Multiple subframes for the same moluid collapse into a single drained
  notification — the worker queries the DB for the full state of the stack,
  not the contents of the notification.

---

## 3. Worker tick, adaptive timeout, and cleanup

The primary complete-stack path is notification-driven: the worker polls Redis,
then queries the DB for each notified `moluid`. Each tick also runs an active-row
sweep so complete stacks can still reach terminal state if a notification was
missed. If a stack is still incomplete, its rows remain `active` until another
notification arrives or the adaptive timeout sweep finalizes it.

Timeout finalization is currently adaptive and uses reduced-row insert time
(`created_at`) as the approximate reduced-subframe arrival time. The first
arriving frame's `EXPTIME` is treated as the exposure duration for the whole
stack. Before a second reduced row arrives, the stack times out after
`EXPTIME + 60s`. Once two rows exist, the worker computes the adjusted baseline
as `(arrival_2 - arrival_1) - EXPTIME`. If that adjusted baseline is zero or
negative, it falls back to `60s`. Later observed reduced-row gaps, and the
currently missing next-frame gap, time out the stack when their adjusted gap
exceeds twice that baseline.

```mermaid
flowchart TD
    tick["Worker tick"]
    drain["Drain Redis notifications<br/>for this camera"]
    any_notify{"Any moluids?"}
    each["For each deduped moluid"]
    load["Load subframes<br/>for moluid from DB"]
    complete{"Complete?"}
    mark_complete["Mark status complete"]
    keep_active["Leave active"]
    sweep["Adaptive timeout scan<br/>active rows for camera"]
    timed_out{"Timed out?"}
    mark_timeout["Mark status timeout"]
    cleanup["Cleanup old terminal rows"]
    sleep["Sleep poll interval"]

    tick --> drain --> any_notify
    any_notify -- "yes" --> each --> load --> complete
    complete -- "yes" --> mark_complete --> sweep
    complete -- "no" --> keep_active --> sweep
    any_notify -- "no" --> sweep
    sweep --> timed_out
    timed_out -- "yes" --> mark_timeout --> cleanup
    timed_out -- "no" --> cleanup
    cleanup --> sleep --> tick
```

`retention_days` defaults to 30 (see `stacking._stacking_worker_arg_parser` for
the current default). Cleanup only removes terminal rows, so incomplete stacks
remain active until completed by a later notification or finalized by adaptive
timeout handling.

---

## 4. Design rationale: why RabbitMQ and Redis?

The current implementation uses RabbitMQ for the inbound site queue and Redis for
two separate roles:

- **RabbitMQ (`STACK_QUEUE_NAME`)** carries inbound subframe messages from the
  producer to `SubframeListener`. The listener validates each payload, submits
  a Celery task, then acks the RabbitMQ message.

- **Redis as Celery broker (`TASK_HOST`)** carries the `process_subframe`
  Celery tasks consumed by `banzai-subframe-worker`.

- **Redis (`stack:notify:{camera}`)** carries *coalesced state-change
  notifications*. The stacking worker does not need to see every individual
  subframe — it needs to know "the state of moluid X may have changed,
  re-query the DB." If three subframes for the same moluid arrive within one
  poll interval, we want to handle them as **one** decision, not three.
  `drain_notifications` collapses duplicates with a `set`, which is
  effectively free with Redis lists + `RENAME`.

- **The DB is the source of truth.** The notification path converges on
  `get_subframes(moluid)` and the `check_stack_complete` predicate. The
  notification only tells the worker *when to look*; it does not carry
  decision-making state.

### Alternatives we did not take

- **Single Celery task per "stack maybe complete" event.** Would force
  per-frame fan-out (no coalescing) and put decision logic on workers that
  don't have per-camera affinity. We'd have to add a per-moluid lock to
  prevent races, which is exactly what the per-camera worker process already
  gives us for free.
- **PostgreSQL `LISTEN/NOTIFY` instead of Redis.** Workable, but coalescing
  is harder (every NOTIFY is delivered) and it couples notification
  semantics to whichever DB session is open. Redis lists are a more natural
  fit for "drain whatever's accumulated since I last looked."
- **Pure polling (no Redis at all).** Simpler, but each worker would have
  to poll the DB at a rate fast enough to feel responsive. This is likely the
  simplest operational model if the query is scoped by camera/status and backed
  by an appropriate index.

If the supervisor/worker architecture ever needs to go away entirely, the natural
endpoint is a Celery task on subframe-completion that does the
`check_stack_complete` decision inline, plus a separate periodic Celery beat
task for timeout/fallback discovery. That is a larger refactor and explicitly
out of scope for the initial smart-stacking PR.

---

## 5. Data model: `subframes`

Defined in `banzai/dbs.py` as `Subframe`. Unique constraint on
`(moluid, stack_num)`.

| Column | Type | Purpose |
|--------|------|---------|
| `moluid` | str | Stack group identifier (FITS `MOLUID`). |
| `stack_num` | int | Position in stack (FITS `MOLFRNUM`). |
| `frmtotal` | int | Expected subframe count (FITS `FRMTOTAL`). |
| `camera` | str | Camera identifier (FITS `INSTRUME`). |
| `filepath` | str | Path to the reduced subframe. |
| `is_last` | bool | Instrument signalled the final subframe of the sequence. |
| `status` | str | `'active'`, `'complete'`, or `'timeout'`. |
| `dateobs` | datetime | Observation timestamp (FITS `DATE-OBS`). |
| `exptime` | float / NULL | Exposure duration in seconds (FITS `EXPTIME`); the first arriving row drives adaptive timeout calculations. |
| `created_at` | datetime | Row creation time. |
| `completed_at` | datetime / NULL | Set when `status` transitions to `'complete'` or `'timeout'`. |

A stack is **complete** when `len(subframes) == frmtotal` OR `any(is_last)` —
see `stacking.check_stack_complete`. New rows are only inserted after
reduction succeeds.

---

## 6. Code map

| File | Contains |
|------|----------|
| `banzai/main.py` | `SubframeListener`, `run_subframe_worker` entry point. |
| `banzai/scheduling.py` | `process_subframe` Celery task. |
| `banzai/stacking.py` | `validate_message`, `check_stack_complete`, `drain_notifications`, `run_worker_loop`, `StackingSupervisor`, `run_supervisor`. |
| `banzai/dbs.py` | `Subframe` model, `insert_subframe`, `get_subframes`, `mark_stack_complete`, `cleanup_old_subframes`. |
| `banzai/settings.py` | `SUBFRAME_TASK_QUEUE_NAME`, `STACK_QUEUE_NAME`, `REDIS_URL`, `RABBITMQ_URL`. |
| `docker-compose-site.yml` | `banzai-subframe-listener`, `banzai-subframe-worker`, `banzai-stacking-supervisor` services. |
| `pyproject.toml` | `banzai_subframe_worker`, `banzai_stacking_supervisor` console entry points. |
| `banzai/tests/test_smart_stacking.py` | Coverage for DB operations, status transitions, notifications, completion logic, listener dispatch, and supervisor spawning. |
