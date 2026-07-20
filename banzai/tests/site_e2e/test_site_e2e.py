"""End-to-end tests for site deployment caching system."""

import os
import shutil
import subprocess
import json
from pathlib import Path

import pytest
import requests
from kombu import Connection, Exchange, Queue
from sqlalchemy import create_engine, text
from astropy.io import fits

from banzai import dbs
from banzai.cache.replication import SUBSCRIPTION_NAME
from banzai.tests.site_e2e.utils import populate_publication
from banzai.tests.site_e2e.conftest import (
    PUBLICATION_DB_ADDRESS, LOCAL_DB_ADDRESS, RAW_DIR, CACHE_DIR, OUTPUT_DIR,
    ARCHIVE_API_URL, REPO_ROOT, SITE_COMPOSE_FILE, SITE_PROJECT_NAME,
    wait_for_subscription_active, wait_for_service_exit, poll_until,
    run_site_compose, drop_subscription_keep_slot,
)
from banzai.utils.messaging import post_to_archive_queue, publish_raw_string_to_queue


# Expected calibration filenames for phase 1 (7 files - top 2 per config + BPM)
PHASE1_EXPECTED_FILES = {
    'lsc0m476-sq34-20260121-bias-central30x30-bin1x1.fits.fz',
    'lsc0m476-sq34-20260120-bias-central30x30-bin1x1.fits.fz',
    'lsc0m476-sq34-20260121-dark-central30x30-bin1x1.fits.fz',
    'lsc0m476-sq34-20260120-dark-central30x30-bin1x1.fits.fz',
    'lsc0m476-sq34-20260121-skyflat-central30x30-bin1x1-V.fits.fz',
    'lsc0m476-sq34-20260118-skyflat-central30x30-bin1x1-V.fits.fz',
    'lsc0m409-sq34-20240314-bpm-central30x30.fits.fz',
}

# Raw science frame to process
RAW_FRAME_ID = 90985172
RAW_FRAME_FILENAME = 'lsc0m476-sq34-20260121-0190-e00.fits.fz'


def _assert_cache_matches(expected_files, timeout=180):
    """Poll until cache contains exactly the expected files with non-zero size."""
    expected = set(expected_files)

    def check():
        if not CACHE_DIR.exists():
            return None
        found = {p.name for p in CACHE_DIR.rglob('*.fits.fz')}
        return found if found == expected else None

    result = poll_until(check, timeout, interval=2)
    if not result:
        found = {p.name for p in CACHE_DIR.rglob('*.fits.fz')} if CACHE_DIR.exists() else set()
        assert False, f"Expected {sorted(expected)}, found {sorted(found)}"

    for p in CACHE_DIR.rglob('*.fits.fz'):
        assert p.stat().st_size > 0, f"Cached file is empty: {p}"


# Broker the host-side test process publishes to; the shared banzai-rabbitmq
# container is exposed on localhost:5672 (containers reach it via host.docker.internal).
BROKER_URL = 'amqp://localhost:5672'
# Durable probe queue bound to the shipper fanout exchange so the test can see
# what the supervisor publishes. Not one of the CONTAINER-consumed e2e queues,
# so it is declared/purged by the test itself rather than the conftest guard.
SHIP_PROBE_QUEUE = 'e2e_ship_probe'


def _bind_ship_probe(exchange_name, queue_name=SHIP_PROBE_QUEUE, broker_url=BROKER_URL):
    """Declare and bind a probe queue to the shipper fanout exchange, then purge it.

    Fanout exchanges drop messages when no queue is bound at publish time, so the
    probe must exist *before* the supervisor ships anything. Purging clears any
    residue left on the shared broker by a previous e2e run so the message
    assertions start from an empty queue.
    """
    exchange = Exchange(exchange_name, type='fanout', durable=True)
    queue = Queue(queue_name, exchange=exchange, durable=True)
    with Connection(broker_url) as conn:
        channel = conn.channel()
        exchange(channel).declare()
        bound_queue = queue(channel)
        bound_queue.declare()
        bound_queue.purge()


def _drain_ship_probe(queue_name=SHIP_PROBE_QUEUE, broker_url=BROKER_URL):
    """Return every message currently in the probe queue as decoded dicts, acking each.

    Bodies must be plain-text JSON strings (content_type='text/plain') — the real
    shipper json.loads the raw body itself and permanently rejects kombu-serialized
    dicts, so the content type is asserted here to pin the wire contract.
    """
    messages = []
    with Connection(broker_url) as conn:
        bound_queue = Queue(queue_name)(conn.channel())
        while True:
            message = bound_queue.get()
            if message is None:
                break
            assert message.content_type == 'text/plain', \
                f'Shipper messages must be text/plain, got {message.content_type!r}'
            messages.append(json.loads(message.body))
            message.ack()
    return messages


def _corrupt_file(path):
    """Overwrite an on-disk file that may be owned by a container UID with garbage.

    The reduced e09 files are written from inside the site containers, so the host
    test user may not own them. Overwrite through a throwaway container that mounts
    the reduced-output tree at the same absolute path (mirrors
    ``conftest._clean_data_dir``), which runs as root and always succeeds.
    """
    result = subprocess.run(
        ["docker", "run", "--rm", "-v", f"{OUTPUT_DIR}:{OUTPUT_DIR}", "alpine",
         "sh", "-c", f"printf 'GARBAGE' > '{path}'"],
        capture_output=True, timeout=30,
    )
    assert result.returncode == 0, f"Failed to corrupt {path}: {result.stderr}"


@pytest.mark.e2e_site
class TestSiteE2E:
    """E2E tests for site deployment caching system."""

    @pytest.mark.e2e_site_startup
    def test_01_publication_db_has_publication(self, publication_db):
        """Verify publication DB is running and has the banzai_calibrations publication."""
        engine = create_engine(PUBLICATION_DB_ADDRESS)
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT pubname FROM pg_publication
                WHERE pubname = 'banzai_calibrations'
            """))
            row = result.fetchone()

        assert row is not None, "Publication 'banzai_calibrations' not found"
        assert row.pubname == 'banzai_calibrations'

    @pytest.mark.e2e_site_startup
    def test_02_site_deployment_running(self, site_deployment):
        """Verify all site deployment services are running."""
        result = run_site_compose("ps", "--format", "json")
        assert result.returncode == 0, f"docker compose ps failed: {result.stderr}"

        ps_output = result.stdout.lower()
        for service in ['banzai-postgresql', 'banzai-download-worker']:
            assert service in ps_output, f"Service {service} not found in docker compose ps output"

    @pytest.mark.e2e_site_startup
    def test_03_replication_subscription_active(self, site_deployment):
        """Verify replication subscription is active."""
        assert wait_for_subscription_active(timeout=60), \
            "Replication subscription did not become active within timeout"

        engine = create_engine(LOCAL_DB_ADDRESS)
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT s.subname, s.subenabled, ss.pid
                FROM pg_subscription s
                LEFT JOIN pg_stat_subscription ss ON s.subname = ss.subname
            """))
            rows = result.fetchall()

        assert len(rows) > 0, "No subscriptions found"
        assert any(row.subenabled and row.pid is not None for row in rows), \
            "No active subscriptions found"

    @pytest.mark.e2e_site_cache
    def test_04_initial_data_replicated(self, site_deployment):
        """Verify initial 7 calibrations replicated to local DB."""
        def check():
            with dbs.get_session(LOCAL_DB_ADDRESS) as session:
                cal_count = session.query(dbs.CalibrationImage).filter(
                    dbs.CalibrationImage.is_master == True,
                    dbs.CalibrationImage.is_bad == False
                ).count()
            return cal_count if cal_count == 7 else None

        result = poll_until(check, timeout=60, interval=2)
        assert result == 7, (
            "Expected 7 calibrations in local DB. "
            "Replication may not have completed."
        )

    @pytest.mark.e2e_site_cache
    def test_05_initial_files_cached(self, site_deployment):
        """Verify download worker cached the 7 calibration files."""
        _assert_cache_matches(PHASE1_EXPECTED_FILES, timeout=180)

    @pytest.mark.e2e_site_reduction
    def test_06_queue_raw_frame(self, site_deployment, auth_token):
        """Download raw science frame and queue it for processing."""
        raw_dir = RAW_DIR
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_frame_path = raw_dir / RAW_FRAME_FILENAME

        api_url = f"{ARCHIVE_API_URL}frames/{RAW_FRAME_ID}/"
        headers = {"Authorization": f"Token {auth_token}"}

        response = requests.get(api_url, headers=headers, timeout=30)
        assert response.status_code == 200, f"Failed to get frame metadata: {response.status_code}"

        fits_url = response.json().get('url')
        assert fits_url, "No 'url' field in frame metadata"

        fits_response = requests.get(fits_url, timeout=120)
        assert fits_response.status_code == 200, f"Failed to download FITS: {fits_response.status_code}"

        raw_frame_path.write_bytes(fits_response.content)
        assert raw_frame_path.stat().st_size > 0, "Downloaded raw frame is empty"

        fits_exchange = os.environ.get('FITS_EXCHANGE', 'fits_files')

        with fits.open(raw_frame_path) as hdul:
            header = hdul['SCI'].header
            site_id = header['SITEID'].strip()
            instrument = header['INSTRUME'].strip()

        post_to_archive_queue(
            filename=raw_frame_path.name,
            broker_url='amqp://localhost:5672',
            exchange_name=fits_exchange,
            path=str(raw_frame_path),
            SITEID=site_id,
            INSTRUME=instrument,
        )

    @pytest.mark.e2e_site_reduction
    def test_07_reduction_completes(self, site_deployment):
        """Verify reduction completed by checking for processed output file."""

        raw_dir = RAW_DIR
        assert raw_dir.exists(), f"Raw directory not found: {raw_dir}"

        raw_files = list(raw_dir.glob('*.fits.fz'))
        assert len(raw_files) > 0, f"No raw FITS files found in {raw_dir}"

        timeout = 300
        failures = []

        for raw_path in raw_files:
            with fits.open(str(raw_path)) as hdul:
                header = hdul['SCI'].header
                site = header['SITEID'].strip().lower()
                instrument = header['INSTRUME'].strip().lower()
                day_obs = header['DAY-OBS'].replace('-', '')

            output_dir = OUTPUT_DIR / site / instrument / day_obs / 'processed'
            expected_name = raw_path.name.replace('-e00.fits.fz', '-e91.fits.fz')
            expected_path = output_dir / expected_name

            found = poll_until(
                lambda p=expected_path: p.exists() and p.stat().st_size > 0,
                timeout, interval=5
            )

            if not found:
                with dbs.get_session(LOCAL_DB_ADDRESS) as session:
                    pattern = raw_path.name.replace('-e00.fits.fz', '%')
                    rows = session.execute(text(
                        "SELECT filename, success FROM processedimages WHERE filename LIKE :pattern"
                    ), {'pattern': pattern}).fetchall()

                if rows:
                    db_info = ", ".join(f"{r.filename} (success={r.success})" for r in rows)
                    failures.append(f"  {raw_path.name}: DB records: {db_info}, expected: {expected_path}")
                else:
                    failures.append(f"  {raw_path.name}: No output or DB record, expected: {expected_path}")

        if failures:
            pytest.fail(f"Reduction failed for {len(failures)}/{len(raw_files)} files:\n" + "\n".join(failures))

    @pytest.mark.e2e_site_reduction
    def test_08_reduction_used_cached_calibrations(self, site_deployment):
        """Verify reduced frames used calibrations that exist in the local cache and DB."""

        output_dir = OUTPUT_DIR
        reduced_files = list(output_dir.rglob('*-e91.fits.fz'))
        assert reduced_files, f"No reduced files found under {output_dir}"

        cal_header_keys = {'L1IDBIAS': 'bias', 'L1IDDARK': 'dark', 'L1IDFLAT': 'flat'}
        cached_files = {p.name for p in CACHE_DIR.rglob('*.fits.fz')}
        errors = []

        for reduced_path in reduced_files:
            with fits.open(str(reduced_path)) as hdul:
                ext = 'SCI' if 'SCI' in hdul else 0
                header = hdul[ext].header

            for key, cal_type in cal_header_keys.items():
                val = header.get(key, '')
                if not val or val == 'N/A':
                    continue
                basename = os.path.basename(val)

                if basename not in cached_files:
                    errors.append(
                        f"{reduced_path.name}: {cal_type} file '{basename}' not found in cache"
                    )

                with dbs.get_session(LOCAL_DB_ADDRESS) as session:
                    cal = session.query(dbs.CalibrationImage).filter(
                        dbs.CalibrationImage.filename == basename
                    ).first()
                cache_root = str(CACHE_DIR)
                if not cal or not cal.filepath or not cal.filepath.startswith(cache_root):
                    errors.append(
                        f"{reduced_path.name}: {cal_type} file '{basename}' filepath="
                        f"{cal.filepath if cal else None!r}, expected under {cache_root}"
                    )

        assert not errors, "Cached calibration verification failed:\n" + "\n".join(errors)

    @pytest.mark.e2e_site_cache
    @pytest.mark.parametrize("drifted_value", [None, "/archive/engineering/fake/path"])
    def test_08b_filepath_reconciled_after_drift(self, site_deployment, drifted_value):
        """Worker restores local filepath when DB value drifts for an on-disk cal."""
        target = 'lsc0m476-sq34-20260121-bias-central30x30-bin1x1.fits.fz'

        with dbs.get_session(LOCAL_DB_ADDRESS) as session:
            cal = session.query(dbs.CalibrationImage).filter_by(filename=target).first()
            assert cal and cal.filepath and cal.filepath.startswith(str(CACHE_DIR)), (
                f"pre-condition failed: {target} not locally cached before drift "
                f"(filepath={cal.filepath if cal else None!r})"
            )
            expected = cal.filepath
            cal.filepath = drifted_value

        def check():
            with dbs.get_session(LOCAL_DB_ADDRESS) as session:
                cal = session.query(dbs.CalibrationImage).filter_by(filename=target).first()
                return cal.filepath if cal and cal.filepath == expected else None

        assert poll_until(check, timeout=60, interval=2) == expected, (
            f"worker did not reconcile {target} back to {expected} after drift to {drifted_value!r}"
        )

    @pytest.mark.e2e_site_cache
    def test_09_add_older_calibrations(self):
        """Insert older calibrations to test cache updates."""
        populate_publication.insert_additional_calibrations(PUBLICATION_DB_ADDRESS)

        with dbs.get_session(PUBLICATION_DB_ADDRESS) as session:
            cal_count = session.query(dbs.CalibrationImage).filter(
                dbs.CalibrationImage.is_master == True,
                dbs.CalibrationImage.is_bad == False
            ).count()

        assert cal_count == 13, (
            f"Expected 13 calibrations in publication DB after adding older ones, "
            f"found {cal_count}"
        )

    @pytest.mark.e2e_site_cache
    def test_10_older_calibrations_replicated(self):
        """Verify new calibrations replicated (now 13 total in DB)."""
        def check():
            with dbs.get_session(LOCAL_DB_ADDRESS) as session:
                count = session.query(dbs.CalibrationImage).filter(
                    dbs.CalibrationImage.is_master == True,
                    dbs.CalibrationImage.is_bad == False
                ).count()
            return count == 13

        assert poll_until(check, timeout=60, interval=2), \
            "Expected 13 calibrations in local DB after replication"

    @pytest.mark.e2e_site_cache
    def test_11_cache_updated(self):
        """Verify cache settled to exactly 7 files after older calibrations added.

        The download worker keeps only the top 2 per config, so the 6 older
        calibrations should not persist in the cache.
        """
        _assert_cache_matches(PHASE1_EXPECTED_FILES, timeout=120)

    @pytest.mark.e2e_site_reduction
    def test_12_stackframe_stack_completes(self, site_deployment):
        """Verify stackframe stacking processes a frame end-to-end.

        Publishes as a raw JSON string to match how instruments send messages.
        """

        raw_dir = RAW_DIR
        src_path = raw_dir / RAW_FRAME_FILENAME
        stackframe_path = raw_dir / 'lsc0m476-sq34-20260121-0301-e00.fits.fz'

        assert src_path.exists(), f"Raw frame not found: {src_path}"
        shutil.copy2(str(src_path), str(stackframe_path))

        with fits.open(str(stackframe_path), mode='update') as hdul:
            hdul['SCI'].header['MOLUID'] = 'mol-e2e-test'
            hdul['SCI'].header['MOLFRNUM'] = 1
            hdul['SCI'].header['FRMTOTAL'] = 1
            hdul['SCI'].header['STACK'] = 'T'

        body = json.dumps({
            'fits_file': str(stackframe_path),
            'last_frame': True,
        })
        stack_queue = os.environ.get('STACK_QUEUE_NAME', 'banzai_stack_queue')
        publish_raw_string_to_queue(stack_queue, body)

        def check():
            with dbs.get_session(LOCAL_DB_ADDRESS, site_deploy=True) as session:
                stack = session.query(dbs.Stack).filter(
                    dbs.Stack.moluid == 'mol-e2e-test'
                ).one_or_none()
                if stack and stack.status == 'complete':
                    stackframes = session.query(dbs.Stackframe).filter(
                        dbs.Stackframe.moluid == 'mol-e2e-test'
                    ).order_by(dbs.Stackframe.stack_num).all()
                    return [s.filepath for s in stackframes]
                return None

        filepaths = poll_until(check, timeout=300)
        assert filepaths, "Stackframe stack did not complete within timeout"

        # Verify the reduced output file exists on disk.
        # Paths in the DB are now absolute host paths, so check directly.
        assert filepaths[0], "Stackframe has no filepath after completion"
        expected_path = Path(filepaths[0])

        found = poll_until(
            lambda p=expected_path: p.exists() and p.stat().st_size > 0,
            timeout=60, interval=5
        )
        assert found, f"Reduced stackframe output not found: {expected_path}"

    @pytest.mark.e2e_site_reduction
    def test_13_smartstack_completes(self, site_deployment):
        """Drive a 3-frame smartstack end-to-end: previews, e45 FITS, JPEGs, ship messages.

        Publishes three stackframe messages (MOLFRNUM 1..3, FRMTOTAL=3, STACK='T',
        last_frame only on the third) as raw JSON strings, the way an instrument
        sends them. The final FITS and every JPEG preview use the first stackframe's
        basename with e45 as the reduction level.
        """
        shipper_exchange = os.environ.get('SHIPPER_EXCHANGE', 'ship_files')
        _bind_ship_probe(shipper_exchange)

        try:
            raw_dir = RAW_DIR
            src_path = raw_dir / RAW_FRAME_FILENAME
            assert src_path.exists(), f"Raw frame not found: {src_path}"

            moluid = 'mol-e2e-stack'
            stack_queue = os.environ.get('STACK_QUEUE_NAME', 'banzai_stack_queue')

            def publish_stackframe(stack_num, is_last):
                frame_path = raw_dir / f'lsc0m476-sq34-20260121-{stack_num:04d}-e00.fits.fz'
                shutil.copy2(str(src_path), str(frame_path))
                with fits.open(str(frame_path), mode='update') as hdul:
                    hdul['SCI'].header['MOLUID'] = moluid
                    hdul['SCI'].header['MOLFRNUM'] = stack_num
                    hdul['SCI'].header['FRMTOTAL'] = 3
                    hdul['SCI'].header['STACK'] = 'T'
                body = json.dumps({
                    'fits_file': str(frame_path),
                    'last_frame': is_last,
                    'instrument_enqueue_timestamp': int(time.time() * 1000),
                })
                publish_raw_string_to_queue(stack_queue, body)

            def preview_rendered(minimum_count):
                with dbs.get_session(LOCAL_DB_ADDRESS, site_deploy=True) as session:
                    stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == moluid).one_or_none()
                    return stack is not None and stack.last_preview_count >= minimum_count

            # Drive two distinct previews so the test proves both publishes reuse the
            # same paths. Reduction makes network calls, so each wait is deliberately generous.
            publish_stackframe(1, is_last=False)
            assert poll_until(lambda: preview_rendered(1), timeout=600, interval=5), \
                "Smartstack never rendered a preview for the first frame"
            publish_stackframe(2, is_last=False)
            assert poll_until(lambda: preview_rendered(2), timeout=600, interval=5), \
                "Smartstack never rendered a preview for the first two frames"

            # Publish the final frame; the stack should now complete and ship the e45.
            publish_stackframe(3, is_last=True)

            def stack_complete():
                with dbs.get_session(LOCAL_DB_ADDRESS, site_deploy=True) as session:
                    stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == moluid).one_or_none()
                    return stack is not None and stack.status == 'complete'

            assert poll_until(stack_complete, timeout=600, interval=5), \
                "Smartstack did not reach status='complete' within timeout"

            # The first stackframe-based e45 FITS and both JPEGs exist under the processed dir.
            fits_matches = list(OUTPUT_DIR.rglob('*-0001-e45.fits'))
            assert len(fits_matches) == 1, f"Expected exactly one e45 FITS, found {fits_matches}"
            e45_path = fits_matches[0]
            assert e45_path.stat().st_size > 0, f"e45 FITS is empty: {e45_path}"

            base = e45_path.name[:-len('.fits')]
            large_jpg = e45_path.with_name(f'{base}-large_thumbnail.jpg')
            small_jpg = e45_path.with_name(f'{base}-small_thumbnail.jpg')
            assert large_jpg.exists() and large_jpg.stat().st_size > 0, f"Missing large JPEG: {large_jpg}"
            assert small_jpg.exists() and small_jpg.stat().st_size > 0, f"Missing small JPEG: {small_jpg}"

            # Drain the shipper probe: previews carry fits=null, the final carries the e45 path.
            time.sleep(3)  # let the final publish settle into the queue
            messages = _drain_ship_probe()
            preview_messages = [m for m in messages if m.get('fits') is None]
            final_messages = [m for m in messages if m.get('fits') is not None]
            assert len(preview_messages) >= 2, \
                f"Expected >=2 preview messages with fits=null, got {messages}"
            assert len(final_messages) == 1, \
                f"Expected exactly one final message with a fits path, got {final_messages}"
            assert final_messages[0]['fits'].endswith(e45_path.name), \
                f"Final ship message fits path {final_messages[0]['fits']!r} does not end with {e45_path.name}"
            assert {Path(message['small_thumbnail']).name for message in messages} == {small_jpg.name}
            assert {Path(message['large_thumbnail']).name for message in messages} == {large_jpg.name}
        finally:
            # Don't leave a durable queue bound to the fanout exchange after the run.
            with Connection(BROKER_URL) as conn:
                Queue(SHIP_PROBE_QUEUE)(conn.channel()).delete()

    @pytest.mark.e2e_site_reduction
    def test_14_poison_stack_self_heals(self, site_deployment):
        """A stack with a corrupted reduced input self-heals to 'error'; a sibling completes.

        After the poison stack's first frame reduces, its e09 file is corrupted so
        finalize cannot open it. Finalize failure is a *caught* exception (not a
        violent crash), so the worker keeps running: it burns one claimed attempt per
        tick and, after the FINALIZE_BACKOFF_SECONDS ladder (set short in site_e2e.env),
        marks the stack 'error'. A healthy sibling published in the same window still
        completes, proving one bad stack does not block the camera's other work.
        """
        raw_dir = RAW_DIR
        src_path = raw_dir / RAW_FRAME_FILENAME
        assert src_path.exists(), f"Raw frame not found: {src_path}"

        stack_queue = os.environ.get('STACK_QUEUE_NAME', 'banzai_stack_queue')
        poison_moluid = 'mol-e2e-poison'
        sibling_moluid = 'mol-e2e-poison-sibling'

        def publish_stackframe(moluid, stack_num, frmtotal, is_last, filename):
            frame_path = raw_dir / filename
            shutil.copy2(str(src_path), str(frame_path))
            with fits.open(str(frame_path), mode='update') as hdul:
                hdul['SCI'].header['MOLUID'] = moluid
                hdul['SCI'].header['MOLFRNUM'] = stack_num
                hdul['SCI'].header['FRMTOTAL'] = frmtotal
                hdul['SCI'].header['STACK'] = 'T'
            body = json.dumps({
                'fits_file': str(frame_path),
                'last_frame': is_last,
                'instrument_enqueue_timestamp': int(time.time() * 1000),
            })
            publish_raw_string_to_queue(stack_queue, body)

        # 1. Publish the poison stack's first frame and wait for it to reduce.
        publish_stackframe(poison_moluid, 1, 2, False, 'lsc0m476-sq34-20260121-0101-e00.fits.fz')

        def poison_frame_reduced():
            with dbs.get_session(LOCAL_DB_ADDRESS, site_deploy=True) as session:
                row = session.query(dbs.Stackframe).filter(
                    dbs.Stackframe.moluid == poison_moluid,
                    dbs.Stackframe.stack_num == 1,
                ).one_or_none()
                return row.filepath if row else None

        # Generous timeout: the reduction path makes network calls (WCS/photometry over
        # VPN) and can stall well past a single frame's typical ~20s under contention.
        e09_path = poll_until(poison_frame_reduced, timeout=600, interval=5)
        assert e09_path, "Poison stack's first frame never reduced"

        # 2. Corrupt the reduced e09 so finalize cannot open it.
        _corrupt_file(e09_path)

        # 3. Complete the poison stack, and publish a healthy sibling in the same window.
        publish_stackframe(poison_moluid, 2, 2, True, 'lsc0m476-sq34-20260121-0102-e00.fits.fz')
        publish_stackframe(sibling_moluid, 1, 2, False, 'lsc0m476-sq34-20260121-0201-e00.fits.fz')
        publish_stackframe(sibling_moluid, 2, 2, True, 'lsc0m476-sq34-20260121-0202-e00.fits.fz')

        # 4. The poison stack exhausts the attempt ladder and lands in 'error'.
        def poison_errored():
            with dbs.get_session(LOCAL_DB_ADDRESS, site_deploy=True) as session:
                stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == poison_moluid).one_or_none()
                return stack is not None and stack.status == 'error'

        assert poll_until(poison_errored, timeout=300, interval=5), \
            "Poison stack did not self-heal to status='error' after the attempt ladder"

        # 5. The healthy sibling still completes despite the poison stack failing.
        def sibling_complete():
            with dbs.get_session(LOCAL_DB_ADDRESS, site_deploy=True) as session:
                stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == sibling_moluid).one_or_none()
                return stack is not None and stack.status == 'complete'

        assert poll_until(sibling_complete, timeout=600, interval=5), \
            "Healthy sibling stack did not complete while the poison stack was failing"

    @pytest.mark.e2e_site_reduction
    def test_15_worker_violent_death_recovers(self, site_deployment):
        """Prove the crash-only supervisor design end-to-end.

        ``run_worker_loop`` catches every exception, so a worker can only end by
        violent death. This test uses ``kill -9`` on the per-camera worker processes
        to stand in for an OOM-kill or segfault. When a worker dies, the supervisor
        exits non-zero and Docker's ``restart: always`` brings the container back with
        a fresh set of workers — the recovery the crash-only design promises. After the
        restart, a newly published stack must still reduce and finalize normally.
        """
        supervisor = 'e2e-banzai-stacking-supervisor'
        before = int(subprocess.run(
            ["docker", "inspect", "-f", "{{.RestartCount}}", supervisor],
            capture_output=True, text=True, timeout=30).stdout.strip())

        # Kill every non-PID-1 python process (the per-camera workers). pgrep/pkill may
        # not exist in the image, so scan /proc. Do not assert on the return code:
        # docker exec can report nonzero when the container exits underneath it.
        subprocess.run(
            ["docker", "exec", supervisor, "sh", "-c",
             "for d in /proc/[0-9]*; do pid=${d#/proc/}; "
             "[ \"$pid\" = \"1\" ] && continue; "
             "grep -q python \"$d/cmdline\" 2>/dev/null && kill -9 \"$pid\"; done"],
            capture_output=True, timeout=30)

        def restarted():
            result = subprocess.run(
                ["docker", "inspect", "-f", "{{.RestartCount}} {{.State.Running}}", supervisor],
                capture_output=True, text=True, timeout=30)
            count_str, _, running = result.stdout.strip().partition(' ')
            return running == 'true' and int(count_str) > before

        assert poll_until(restarted, timeout=120, interval=5), \
            "Supervisor container did not restart after worker was killed"

        raw_dir = RAW_DIR
        src_path = raw_dir / RAW_FRAME_FILENAME
        assert src_path.exists(), f"Raw frame not found: {src_path}"

        moluid = 'mol-e2e-restart'
        stack_queue = os.environ.get('STACK_QUEUE_NAME', 'banzai_stack_queue')

        def publish_stackframe(stack_num, is_last, filename):
            frame_path = raw_dir / filename
            shutil.copy2(str(src_path), str(frame_path))
            with fits.open(str(frame_path), mode='update') as hdul:
                hdul['SCI'].header['MOLUID'] = moluid
                hdul['SCI'].header['MOLFRNUM'] = stack_num
                hdul['SCI'].header['FRMTOTAL'] = 2
                hdul['SCI'].header['STACK'] = 'T'
            body = json.dumps({
                'fits_file': str(frame_path),
                'last_frame': is_last,
                'instrument_enqueue_timestamp': int(time.time() * 1000),
            })
            publish_raw_string_to_queue(stack_queue, body)

        publish_stackframe(1, False, 'lsc0m476-sq34-20260121-0401-e00.fits.fz')
        publish_stackframe(2, True, 'lsc0m476-sq34-20260121-0402-e00.fits.fz')

        def stack_complete():
            with dbs.get_session(LOCAL_DB_ADDRESS, site_deploy=True) as session:
                stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == moluid).one_or_none()
                return stack is not None and stack.status == 'complete'

        assert poll_until(stack_complete, timeout=600, interval=5), \
            "Stack did not complete after supervisor restart"

    @pytest.mark.e2e_site_startup
    def test_16_cache_init_reuses_existing_slot(self, site_deployment):
        """Verify cache-init succeeds when the replication slot already exists on the publisher."""
        drop_subscription_keep_slot(LOCAL_DB_ADDRESS, SUBSCRIPTION_NAME)

        run_site_compose("rm", "-f", "banzai-cache-init")
        result = run_site_compose("up", "-d", "banzai-cache-init")
        assert result.returncode == 0, f"Failed to restart cache-init: {result.stderr}"

        assert wait_for_service_exit(
            SITE_COMPOSE_FILE, "banzai-cache-init", expected_code=0, timeout=60,
            cwd=REPO_ROOT, project=SITE_PROJECT_NAME,
        ), "cache-init did not exit successfully after reusing existing slot"

        assert wait_for_subscription_active(timeout=60), \
            "Replication subscription did not become active after slot reuse"
