"""End-to-end tests for site deployment caching system."""

import os
import subprocess
from pathlib import Path

import pytest
import requests
from sqlalchemy import create_engine, text

from banzai import dbs
from banzai.tests.site_e2e.utils import populate_publication
from banzai.tests.site_e2e.conftest import (
    PUBLICATION_DB_ADDRESS, LOCAL_DB_ADDRESS, CACHE_DIR, DATA_DIR,
    ARCHIVE_API_URL, REPO_ROOT,
    wait_for_subscription_active, poll_until, run_site_compose,
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

    result = poll_until(check, timeout)
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
        for service in ['banzai-postgresql', 'banzai-redis', 'banzai-download-worker']:
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

        result = poll_until(check, timeout=60)
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
                'uv', 'run', 'python', str(queue_script),
                str(raw_dir),
                '--broker-url', 'amqp://localhost:5672',
                '--exchange', fits_exchange,
                '--container-path', '/data/raw'
            ],
            capture_output=True, text=True, timeout=30, cwd=str(REPO_ROOT)
        )
        assert result.returncode == 0, f"Failed to queue raw frame: {result.stderr}\n{result.stdout}"
        assert 'Files to process: 1' in result.stdout, f"Expected 1 file to be queued: {result.stdout}"

    @pytest.mark.e2e_site_reduction
    def test_07_reduction_completes(self, site_deployment):
        """Verify reduction completed by checking for processed output file."""
        from astropy.io import fits

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
                timeout, interval=10
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

    @pytest.mark.e2e_site_cache
    def test_08_add_older_calibrations(self):
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
    def test_09_older_calibrations_replicated(self):
        """Verify new calibrations replicated (now 13 total in DB)."""
        def check():
            with dbs.get_session(LOCAL_DB_ADDRESS) as session:
                count = session.query(dbs.CalibrationImage).filter(
                    dbs.CalibrationImage.is_master == True,
                    dbs.CalibrationImage.is_bad == False
                ).count()
            return count == 13

        assert poll_until(check, timeout=60), \
            "Expected 13 calibrations in local DB after replication"

    @pytest.mark.e2e_site_cache
    def test_10_cache_updated(self):
        """Verify cache settled to exactly 7 files after older calibrations added.

        The download worker keeps only the top 2 per config, so the 6 older
        calibrations should not persist in the cache.
        """
        _assert_cache_matches(PHASE1_EXPECTED_FILES, timeout=120)
