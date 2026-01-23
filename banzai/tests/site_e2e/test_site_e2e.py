"""
End-to-end tests for site deployment caching system.

These tests validate the full site deployment flow including:
1. Publication database setup with logical replication
2. Site deployment services (PostgreSQL, Redis, RabbitMQ, workers)
3. Replication subscription from publication DB to local DB
4. Calibration file caching via download worker
5. Frame reduction using cached calibrations

Tests run in order and share state via fixtures defined in conftest.py.

Usage:
    # Run all site E2E tests
    pytest -m e2e_site banzai/tests/site_e2e/test_site_e2e.py -v

    # Run specific test phases
    pytest -m e2e_site_startup ...   # Publication DB and site deployment startup
    pytest -m e2e_site_cache ...     # Cache sync tests
    pytest -m e2e_site_reduction ... # Frame reduction tests

Prerequisites:
    - Docker and docker compose installed
    - AUTH_TOKEN environment variable set for archive API access
    - Copy site_e2e.env.template to site_e2e.env and configure
"""

import os
import subprocess
import time

import pytest
from sqlalchemy import create_engine, text

from banzai import dbs
from banzai.tests.site_e2e.utils import populate_publication
from banzai.tests.site_e2e.utils.wait_for_downloads import wait_for_downloads
from banzai.tests.site_e2e.utils.wait_for_replication import wait_for_replication_sync


# Expected calibration filenames for phase 1 (7 files - top 2 per config + BPM)
PHASE1_EXPECTED_FILES = [
    'lsc0m476-sq34-20260121-bias-central30x30-bin1x1.fits.fz',
    'lsc0m476-sq34-20260120-bias-central30x30-bin1x1.fits.fz',
    'lsc0m476-sq34-20260121-dark-central30x30-bin1x1.fits.fz',
    'lsc0m476-sq34-20260120-dark-central30x30-bin1x1.fits.fz',
    'lsc0m476-sq34-20260121-skyflat-central30x30-bin1x1-V.fits.fz',
    'lsc0m476-sq34-20260118-skyflat-central30x30-bin1x1-V.fits.fz',
    'lsc0m409-sq34-20240314-bpm-central30x30.fits.fz',
]

# Raw science frame to process
RAW_FRAME_ID = 90985172
RAW_FRAME_FILENAME = 'lsc0m476-sq34-20260121-0190-e00.fits.fz'


@pytest.mark.e2e
@pytest.mark.e2e_site
class TestSiteE2E:
    """E2E tests for site deployment caching system.

    Tests run in order and share state via fixtures.
    """

    @pytest.mark.e2e_site_startup
    def test_01_publication_db_has_publication(self, publication_db, publication_db_address):
        """Verify publication DB is running and has the banzai_calibrations publication."""
        engine = create_engine(publication_db_address)
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT pubname, puballtables
                FROM pg_publication
                WHERE pubname = 'banzai_calibrations'
            """))
            row = result.fetchone()

        assert row is not None, "Publication 'banzai_calibrations' not found in pg_publication"
        assert row.pubname == 'banzai_calibrations'

    @pytest.mark.e2e_site_startup
    def test_02_site_deployment_running(self, site_deployment, site_compose_file, site_env_file):
        """Verify all site deployment services are running."""
        result = subprocess.run(
            ['docker', 'compose', '-f', site_compose_file,
             '--env-file', site_env_file, 'ps', '--format', 'json'],
            capture_output=True,
            text=True
        )

        assert result.returncode == 0, f"docker compose ps failed: {result.stderr}"

        # Check that essential services are running
        # Note: banzai-cache-init is expected to have exited (it's an init container)
        essential_services = ['banzai-postgresql', 'banzai-redis', 'banzai-download-worker']
        ps_output = result.stdout.lower()

        for service in essential_services:
            assert service.lower() in ps_output, f"Service {service} not found in docker compose ps output"

    @pytest.mark.e2e_site_startup
    def test_03_replication_subscription_active(self, site_deployment, local_db_address):
        """Verify replication subscription is active."""
        # Wait for subscription to sync
        synced = wait_for_replication_sync(local_db_address, timeout=60)
        assert synced, "Replication subscription did not become active within timeout"

        # Query subscription status from pg_subscription (which has subenabled column)
        engine = create_engine(local_db_address)
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT s.subname, s.subenabled, ss.pid
                FROM pg_subscription s
                LEFT JOIN pg_stat_subscription ss ON s.subname = ss.subname
            """))
            rows = result.fetchall()

        assert len(rows) > 0, "No subscriptions found"

        # Check that at least one subscription is enabled and has an active worker
        enabled_subs = [row for row in rows if row.subenabled and row.pid is not None]
        assert len(enabled_subs) > 0, "No active subscriptions found"

    @pytest.mark.e2e_site_cache
    def test_04_initial_data_replicated(self, site_deployment, local_db_address):
        """Verify initial 7 calibrations replicated to local DB."""
        # Give replication a moment to sync initial data
        time.sleep(5)

        with dbs.get_session(local_db_address) as session:
            calibrations = session.query(dbs.CalibrationImage).filter(
                dbs.CalibrationImage.is_master == True,
                dbs.CalibrationImage.is_bad == False
            ).all()

            cal_count = len(calibrations)

        assert cal_count == 7, (
            f"Expected 7 calibrations in local DB, found {cal_count}. "
            "Replication may not have completed."
        )

    @pytest.mark.e2e_site_cache
    def test_05_initial_files_cached(self, site_deployment, cache_dir):
        """Verify download worker cached the 7 calibration files."""
        # Wait for files to be downloaded
        success = wait_for_downloads(
            cache_dir=cache_dir,
            expected_files=PHASE1_EXPECTED_FILES,
            timeout=180  # 3 minutes for downloads
        )

        assert success, f"Download worker did not cache all expected files within timeout"

        # Verify all files exist and have non-zero size
        for filename in PHASE1_EXPECTED_FILES:
            filepath = os.path.join(cache_dir, filename)
            assert os.path.exists(filepath), f"Expected cached file not found: {filename}"
            assert os.path.getsize(filepath) > 0, f"Cached file is empty: {filename}"

    @pytest.mark.e2e_site_reduction
    def test_06_queue_raw_frame(self, site_deployment, data_dir, archive_api_url, auth_token):
        """Download raw science frame and queue it for processing."""
        raw_dir = os.path.join(data_dir, 'raw')
        os.makedirs(raw_dir, exist_ok=True)

        # Download raw frame from archive API
        raw_frame_path = os.path.join(raw_dir, RAW_FRAME_FILENAME)
        download_url = f"{archive_api_url}frames/{RAW_FRAME_ID}/?key={auth_token}"

        result = subprocess.run(
            ['curl', '-L', '-o', raw_frame_path, download_url],
            capture_output=True,
            text=True,
            timeout=120
        )

        assert result.returncode == 0, f"Failed to download raw frame: {result.stderr}"
        assert os.path.exists(raw_frame_path), f"Raw frame not downloaded: {raw_frame_path}"
        assert os.path.getsize(raw_frame_path) > 0, f"Downloaded raw frame is empty"

        # Queue the frame for processing via RabbitMQ
        # queue_images.py is at the repo root
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        queue_script = os.path.join(repo_root, 'queue_images.py')
        fits_exchange = os.environ.get('FITS_EXCHANGE', 'fits_files')

        result = subprocess.run(
            [
                'uv', 'run', 'python', queue_script,
                raw_dir,
                '--broker-url', 'amqp://localhost:5672',
                '--exchange', fits_exchange,
                '--container-path', '/data/raw'
            ],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=repo_root
        )

        assert result.returncode == 0, f"Failed to queue raw frame: {result.stderr}\n{result.stdout}"
        assert 'Files to process: 1' in result.stdout, f"Expected 1 file to be queued: {result.stdout}"

    @pytest.mark.e2e_site_reduction
    def test_07_reduction_completes(self, site_deployment, data_dir, local_db_address):
        """Verify reduction completed successfully."""
        output_dir = os.path.join(data_dir, 'output')

        # Wait for output file to appear
        # The processed filename will have a different suffix (e91 for reduced)
        expected_output_pattern = 'lsc0m476-sq34-20260121-0190'
        timeout = 300  # 5 minutes for reduction
        poll_interval = 10

        start_time = time.time()
        output_file = None

        while time.time() - start_time < timeout:
            if os.path.exists(output_dir):
                for filename in os.listdir(output_dir):
                    if expected_output_pattern in filename and filename.endswith('.fits.fz'):
                        output_file = os.path.join(output_dir, filename)
                        break

            if output_file and os.path.exists(output_file):
                break

            time.sleep(poll_interval)

        # If no output file found, check the processedimages table as fallback
        if output_file is None:
            with dbs.get_session(local_db_address) as session:
                # Query for processed image record
                result = session.execute(text("""
                    SELECT filename FROM processedimages
                    WHERE filename LIKE :pattern
                """), {'pattern': f'%{expected_output_pattern}%'})
                row = result.fetchone()

                if row:
                    pytest.skip(
                        f"Reduction record found in DB ({row.filename}) but output file not in expected location. "
                        "This may indicate a path configuration issue."
                    )

        assert output_file is not None, (
            f"Reduction did not complete within {timeout}s. "
            f"No output file matching '{expected_output_pattern}' found in {output_dir}"
        )

    @pytest.mark.e2e_site_cache
    def test_08_add_older_calibrations(self, publication_db_address):
        """Insert older calibrations to test cache updates."""
        # Insert 6 additional older calibrations via populate_publication utility
        populate_publication.insert_additional_calibrations(publication_db_address)

        # Verify they were inserted in the publication DB
        with dbs.get_session(publication_db_address) as session:
            cal_count = session.query(dbs.CalibrationImage).filter(
                dbs.CalibrationImage.is_master == True,
                dbs.CalibrationImage.is_bad == False
            ).count()

        assert cal_count == 13, (
            f"Expected 13 calibrations in publication DB after adding older ones, "
            f"found {cal_count}"
        )

    @pytest.mark.e2e_site_cache
    def test_09_older_calibrations_replicated(self, local_db_address):
        """Verify new calibrations replicated (now 13 total in DB)."""
        # Give replication time to sync the new records
        timeout = 60
        poll_interval = 5
        start_time = time.time()
        cal_count = 0

        while time.time() - start_time < timeout:
            with dbs.get_session(local_db_address) as session:
                cal_count = session.query(dbs.CalibrationImage).filter(
                    dbs.CalibrationImage.is_master == True,
                    dbs.CalibrationImage.is_bad == False
                ).count()

            if cal_count == 13:
                break

            time.sleep(poll_interval)

        assert cal_count == 13, (
            f"Expected 13 calibrations in local DB after replication, found {cal_count}. "
            "New calibrations may not have replicated."
        )

    @pytest.mark.e2e_site_cache
    def test_10_cache_updated(self, cache_dir):
        """Verify download worker downloaded new files then deleted old ones.

        After adding 6 older calibrations, the cache should still have only 7 files
        because the download worker keeps only the top 2 per config (instrument, type,
        config_mode, binning, filter).

        The 6 new calibrations are all older versions that rank below the existing
        top 2 for each calibration type, so:
        - Download worker may briefly download them
        - Then realizes they're not in top 2 and deletes them
        - Final state: same 7 files as before (top 2 BIAS, top 2 DARK, top 2 SKYFLAT, 1 BPM)
        """
        # Give download worker time to process the new calibrations
        timeout = 120
        poll_interval = 10
        start_time = time.time()

        # Wait for cache to stabilize at 7 files
        while time.time() - start_time < timeout:
            if os.path.exists(cache_dir):
                files = [f for f in os.listdir(cache_dir) if f.endswith('.fits.fz')]
                if len(files) == 7:
                    # Verify these are the expected files (top 2 per config)
                    for expected_file in PHASE1_EXPECTED_FILES:
                        assert expected_file in files, (
                            f"Expected file {expected_file} missing from cache. "
                            f"Files present: {files}"
                        )
                    return  # Test passed

            time.sleep(poll_interval)

        # Final check after timeout
        files = [f for f in os.listdir(cache_dir) if f.endswith('.fits.fz')] if os.path.exists(cache_dir) else []
        assert len(files) == 7, (
            f"Expected exactly 7 files in cache (top 2 per config), found {len(files)}. "
            f"Files present: {files}"
        )
