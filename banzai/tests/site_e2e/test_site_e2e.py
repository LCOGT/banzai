"""End-to-end tests for site deployment caching system."""

import datetime
import os
import shutil
import subprocess
import sys
import time
import json
from pathlib import Path

import pytest
import requests
from sqlalchemy import create_engine, text
from astropy.io import fits

from banzai import dbs
from banzai.tests.site_e2e.utils import populate_publication
from banzai.tests.site_e2e.conftest import (
    PUBLICATION_DB_ADDRESS, LOCAL_DB_ADDRESS, CACHE_DIR, DATA_DIR,
    ARCHIVE_API_URL, REPO_ROOT, SITE_COMPOSE_FILE,
    wait_for_subscription_active, wait_for_service_exit, poll_until,
    run_site_compose, publish_raw_string_to_queue, drop_subscription_keep_slot,
)


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
        raw_dir = DATA_DIR / 'raw'
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

        queue_script = REPO_ROOT / 'scripts' / 'queue_images.py'
        fits_exchange = os.environ.get('FITS_EXCHANGE', 'fits_files')

        result = subprocess.run(
            [
                sys.executable, str(queue_script),
                str(raw_dir),
                '--broker-url', 'amqp://localhost:5672',
                '--exchange', fits_exchange,
                '--container-path', '/raw'
            ],
            capture_output=True, text=True, timeout=30, cwd=str(REPO_ROOT)
        )
        assert result.returncode == 0, f"Failed to queue raw frame: {result.stderr}\n{result.stdout}"
        assert 'Files to process: 1' in result.stdout, f"Expected 1 file to be queued: {result.stdout}"

    @pytest.mark.e2e_site_reduction
    def test_07_reduction_completes(self, site_deployment):
        """Verify reduction completed by checking for processed output file."""

        raw_dir = DATA_DIR / 'raw'
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

            output_dir = DATA_DIR / 'output' / site / instrument / day_obs / 'processed'
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

        output_dir = DATA_DIR / 'output'
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
                if not cal or not cal.filepath:
                    errors.append(
                        f"{reduced_path.name}: {cal_type} file '{basename}' missing or NULL filepath in DB"
                    )

        assert not errors, "Cached calibration verification failed:\n" + "\n".join(errors)

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
    def test_12_subframe_stack_completes(self, site_deployment):
        """Verify subframe stacking processes a frame end-to-end.

        Publishes as a raw JSON string to match how instruments send messages.
        """

        raw_dir = DATA_DIR / 'raw'
        src_path = raw_dir / RAW_FRAME_FILENAME
        subframe_path = raw_dir / 'subframe_test.fits.fz'

        assert src_path.exists(), f"Raw frame not found: {src_path}"
        shutil.copy2(str(src_path), str(subframe_path))

        with fits.open(str(subframe_path), mode='update') as hdul:
            hdul['SCI'].header['MOLUID'] = 'mol-e2e-test'
            hdul['SCI'].header['MOLFRNUM'] = 1
            hdul['SCI'].header['FRMTOTAL'] = 1
            hdul['SCI'].header['STACK'] = 'T'

        body = json.dumps({
            'fits_file': '/raw/subframe_test.fits.fz',
            'last_frame': True,
            'instrument_enqueue_timestamp': int(time.time() * 1000),
        })
        publish_raw_string_to_queue('banzai_stack_queue', body)

        def check():
            with dbs.get_session(LOCAL_DB_ADDRESS) as session:
                frames = session.query(dbs.StackFrame).filter(
                    dbs.StackFrame.moluid == 'mol-e2e-test'
                ).all()
                if frames and all(f.status == 'complete' for f in frames):
                    return [f.filepath for f in frames]
                return None

        filepaths = poll_until(check, timeout=300)
        assert filepaths, "Subframe stack did not complete within timeout"

        # Verify the reduced output file exists on disk.
        # The container path (e.g. /reduced/lsc/sq34/.../file.fits.fz) maps to DATA_DIR/output/...
        container_path = filepaths[0]
        assert container_path, "StackFrame has no filepath after completion"
        relative_path = container_path.removeprefix('/reduced/')
        expected_path = DATA_DIR / 'output' / relative_path

        found = poll_until(
            lambda p=expected_path: p.exists() and p.stat().st_size > 0,
            timeout=60, interval=5
        )
        assert found, f"Reduced subframe output not found: {expected_path}"

    @pytest.mark.e2e_site_cache
    def test_13_stack_timeout(self, site_deployment):
        """Verify stacking supervisor times out incomplete stacks."""
        stale_dateobs = datetime.datetime.utcnow() - datetime.timedelta(minutes=25)

        for stack_num in [1, 2]:
            dbs.insert_stack_frame(
                LOCAL_DB_ADDRESS,
                moluid='mol-e2e-timeout',
                stack_num=stack_num,
                frmtotal=3,
                camera='sq34',
                filepath='/tmp/fake.fits',
                is_last=False,
                dateobs=stale_dateobs,
            )

        def check():
            with dbs.get_session(LOCAL_DB_ADDRESS) as session:
                frames = session.query(dbs.StackFrame).filter(
                    dbs.StackFrame.moluid == 'mol-e2e-timeout'
                ).all()
                if frames and all(f.status == 'timeout' for f in frames):
                    return frames
                return None

        result = poll_until(check, timeout=60)
        assert result, "Stacking supervisor did not timeout the stale stack"
        assert len(result) == 2, f"Expected 2 timed-out frames, found {len(result)}"

    @pytest.mark.e2e_site_startup
    def test_14_cache_init_reuses_existing_slot(self, site_deployment):
        """Verify cache-init succeeds when the replication slot already exists on the publisher."""
        site_id = os.environ.get("SITE_ID", "lsc")
        subscription_name = f"banzai_{site_id}_sub"

        drop_subscription_keep_slot(LOCAL_DB_ADDRESS, subscription_name)

        run_site_compose("rm", "-f", "banzai-cache-init")
        result = run_site_compose("up", "-d", "banzai-cache-init")
        assert result.returncode == 0, f"Failed to restart cache-init: {result.stderr}"

        assert wait_for_service_exit(
            SITE_COMPOSE_FILE, "banzai-cache-init", expected_code=0, timeout=60,
            cwd=REPO_ROOT
        ), "cache-init did not exit successfully after reusing existing slot"

        assert wait_for_subscription_active(timeout=60), \
            "Replication subscription did not become active after slot reuse"
