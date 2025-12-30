"""
Download worker for calibration file caching.

This module implements an event-driven download worker that processes calibration
files queued by PostgreSQL triggers. The worker runs as a daemon, continuously
polling the pending_downloads queue and downloading FITS files from the archive.

Part of the PostgreSQL replication-based calibration cache system.
See docs/postgresql_replication_cache_design.md for architecture details.
"""

import os
import sys
import time
import datetime
from io import BytesIO

from astropy.io import fits

from banzai import dbs, logs
from banzai.utils import fits_utils
from banzai.exceptions import FrameNotAvailableError

logger = logs.get_logger()


class DownloadWorker:
    """
    Event-driven calibration file download worker.

    Continuously polls the pending_downloads queue and downloads FITS files
    from the archive API. Updates the database with local file paths and
    performs automatic cleanup of old versions.

    Attributes:
        db_address: Database connection string for local site database
        cache_root: Root directory for calibration file storage
        runtime_context: Runtime context with archive API configuration
    """

    def __init__(self, db_address, cache_root, runtime_context):
        """
        Initialize download worker.

        Args:
            db_address: Database connection string (e.g., 'postgresql://user:pass@host/db')
            cache_root: Root directory for cache (e.g., '/data/calibrations')
            runtime_context: Context object with archive API settings
        """
        self.db_address = db_address
        self.cache_root = cache_root
        self.runtime_context = runtime_context

        logger.info(f"Initialized download worker with cache_root: {cache_root}")

    def run(self, poll_interval=10):
        """
        Main worker loop.

        Continuously polls for pending downloads, processes them, and performs
        cleanup. Runs indefinitely until interrupted.

        Args:
            poll_interval: Seconds to wait between queue polls (default: 10)
        """
        logger.info("Starting download worker")

        while True:
            try:
                # Get pending downloads
                pending = self.get_pending_downloads(limit=10)

                if not pending:
                    # No work to do, sleep and continue
                    time.sleep(poll_interval)
                    continue

                logger.info(f"Processing {len(pending)} pending downloads")

                # Process each download
                for download in pending:
                    try:
                        self.process_download(download)
                    except Exception as e:
                        # Log error but continue processing other downloads
                        logger.error(f"Error processing download {download.id}: {e}", exc_info=True)

                # Cleanup old files after processing batch
                try:
                    self.cleanup_old_files()
                except Exception as e:
                    logger.error(f"Error during cleanup: {e}", exc_info=True)

            except KeyboardInterrupt:
                logger.info("Download worker interrupted by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in worker loop: {e}", exc_info=True)
                # Back off on errors to avoid rapid failure loops
                time.sleep(30)

        logger.info("Download worker stopped")

    def get_pending_downloads(self, limit=10):
        """
        Get pending downloads from queue.

        Queries the pending_downloads table for downloads with status='pending',
        ordered by creation time (oldest first).

        Args:
            limit: Maximum number of downloads to return (default: 10)

        Returns:
            List of PendingDownload objects
        """
        with dbs.get_session(self.db_address) as session:
            return session.query(dbs.PendingDownload)\
                .filter_by(status='pending')\
                .order_by(dbs.PendingDownload.created_at)\
                .limit(limit)\
                .all()

    def process_download(self, download):
        """
        Process a single download.

        Downloads the FITS file from the archive, validates it, saves to disk,
        and updates the database with the local file path.

        Args:
            download: PendingDownload object to process

        Raises:
            FrameNotAvailableError: If frame cannot be downloaded from archive
            ValueError: If downloaded file is invalid
            Exception: For other errors during download
        """
        try:
            # Mark as downloading
            self.mark_downloading(download.id)

            # Get calibration record
            cal = self.get_calibration(download.calimage_id)

            if cal is None:
                raise ValueError(f"Calibration {download.calimage_id} not found")

            # Determine local paths
            local_dir = os.path.join(self.cache_root, cal.type)
            local_path = os.path.join(local_dir, cal.filename)

            # Skip if already downloaded
            if os.path.exists(local_path):
                logger.info(f"File already exists: {cal.filename}, updating database")
                with dbs.get_session(self.db_address) as session:
                    cal_record = session.query(dbs.CalibrationImage).get(cal.id)
                    if cal_record:
                        cal_record.filepath = local_dir
                        session.commit()
                self.mark_completed(download.id)
                return

            # Create directory if needed
            os.makedirs(local_dir, exist_ok=True)

            # Download from archive API
            logger.info(f"Downloading {cal.filename} (frameid: {cal.frameid}, type: {cal.type})")

            file_info = {
                'frameid': cal.frameid,
                'filename': cal.filename
            }

            # Download file
            buffer = fits_utils.download_from_s3(
                file_info,
                self.runtime_context,
                is_raw_frame=False
            )

            # Save to disk
            with open(local_path, 'wb') as f:
                f.write(buffer.read())
            buffer.close()

            # Validate FITS file
            if not self.validate_fits(local_path):
                os.remove(local_path)
                raise ValueError("Invalid FITS file")

            # Update filepath in database (store directory, not full path)
            with dbs.get_session(self.db_address) as session:
                cal_record = session.query(dbs.CalibrationImage).get(cal.id)
                if cal_record:
                    cal_record.filepath = local_dir
                    session.commit()

            # Mark download complete
            self.mark_completed(download.id)
            logger.info(f"Successfully downloaded {cal.filename} to {local_dir}")

        except FrameNotAvailableError as e:
            # Frame not available in archive (expected for some calibrations)
            logger.warning(f"Frame not available: {e}")
            self.mark_failed(download.id, str(e))
        except Exception as e:
            # Other errors - retry up to max attempts
            logger.error(f"Download failed for {download.id}: {e}", exc_info=True)
            self.mark_failed(download.id, str(e))

    def validate_fits(self, filepath):
        """
        Validate FITS file structure.

        Opens the file and checks for valid FITS format. Ensures the file
        has at least one HDU with a readable header.

        Args:
            filepath: Path to FITS file to validate

        Returns:
            bool: True if valid FITS file, False otherwise
        """
        try:
            with fits.open(filepath, memmap=False) as hdul:
                # Check we can read the header
                _ = hdul[0].header
                # Check we have at least one HDU
                if len(hdul) == 0:
                    logger.warning(f"FITS file has no HDUs: {filepath}")
                    return False
            return True
        except Exception as e:
            logger.warning(f"FITS validation failed for {filepath}: {e}")
            return False

    def cleanup_old_files(self):
        """
        Remove files marked for deletion.

        Finds calibrations where filepath=NULL (marked for deletion by version
        cleanup trigger) and removes the corresponding files from disk. Searches
        all calibration type directories since we don't know where the file was.
        """
        with dbs.get_session(self.db_address) as session:
            # Find calibrations marked for cleanup (filepath=NULL)
            cals_to_delete = session.query(dbs.CalibrationImage)\
                .filter(dbs.CalibrationImage.filepath == None)\
                .all()

            if not cals_to_delete:
                return

            logger.info(f"Cleaning up {len(cals_to_delete)} old calibration files")

            # Try to find and delete each file
            for cal in cals_to_delete:
                # Try common calibration type directories
                for cal_type in ['BIAS', 'DARK', 'SKYFLAT', 'FLAT', 'BPM', 'READNOISE']:
                    potential_path = os.path.join(self.cache_root, cal_type, cal.filename)

                    if os.path.exists(potential_path):
                        try:
                            os.remove(potential_path)
                            logger.info(f"Deleted old calibration: {cal.filename}")
                        except Exception as e:
                            logger.error(f"Failed to delete {cal.filename}: {e}")
                        break  # File found and processed, move to next calibration

    def get_calibration(self, calimage_id):
        """
        Get calibration record from database.

        Args:
            calimage_id: ID of CalibrationImage record

        Returns:
            CalibrationImage object or None if not found
        """
        with dbs.get_session(self.db_address) as session:
            return session.query(dbs.CalibrationImage).get(calimage_id)

    def mark_downloading(self, download_id):
        """
        Mark download as in progress.

        Updates the status to 'downloading' to indicate work has started.

        Args:
            download_id: ID of PendingDownload record
        """
        with dbs.get_session(self.db_address) as session:
            download = session.query(dbs.PendingDownload).get(download_id)
            if download:
                download.status = 'downloading'
                session.commit()

    def mark_completed(self, download_id):
        """
        Mark download as completed.

        Updates the status to 'completed' and records completion timestamp.

        Args:
            download_id: ID of PendingDownload record
        """
        with dbs.get_session(self.db_address) as session:
            download = session.query(dbs.PendingDownload).get(download_id)
            if download:
                download.status = 'completed'
                download.completed_at = datetime.datetime.utcnow()
                session.commit()

    def mark_failed(self, download_id, error_msg):
        """
        Mark download as failed.

        Increments retry count and stores error message. If under max retries,
        resets status to 'pending' for automatic retry. Otherwise marks as 'failed'.

        Args:
            download_id: ID of PendingDownload record
            error_msg: Error message describing the failure
        """
        max_retries = 3

        with dbs.get_session(self.db_address) as session:
            download = session.query(dbs.PendingDownload).get(download_id)
            if download:
                download.retry_count += 1
                download.error_message = error_msg

                # Retry if under max attempts, otherwise mark as failed
                if download.retry_count < max_retries:
                    download.status = 'pending'  # Retry
                    logger.info(f"Download {download_id} will be retried "
                              f"(attempt {download.retry_count + 1}/{max_retries})")
                else:
                    download.status = 'failed'
                    logger.error(f"Download {download_id} failed after {max_retries} attempts")

                session.commit()


def run_download_worker_daemon():
    """
    Console entry point for download worker daemon.

    Reads configuration from environment variables and starts the worker.
    Runs indefinitely until interrupted with Ctrl-C.

    Environment Variables:
        DB_ADDRESS: Database connection string (required)
        CACHE_FILES_ROOT: Root directory for cache (default: /data/calibrations)
        DOWNLOAD_WORKER_POLL_INTERVAL: Poll interval in seconds (default: 10)

    Exit Codes:
        0: Clean shutdown
        1: Configuration error (missing DB_ADDRESS)
    """
    # Import settings here to avoid circular imports
    from banzai import settings
    from banzai.context import Context

    # Read configuration from environment
    db_address = os.getenv('DB_ADDRESS')
    cache_root = os.getenv('CACHE_FILES_ROOT', '/data/calibrations')
    poll_interval = int(os.getenv('DOWNLOAD_WORKER_POLL_INTERVAL', '10'))

    if not db_address:
        logger.error('DB_ADDRESS environment variable not set')
        sys.exit(1)

    logger.info(f"Starting download worker daemon")
    logger.info(f"Database: {db_address}")
    logger.info(f"Cache root: {cache_root}")
    logger.info(f"Poll interval: {poll_interval}s")

    # Create minimal context for downloading
    # This provides archive API configuration
    runtime_context = Context({
        'ARCHIVE_FRAME_URL': settings.ARCHIVE_FRAME_URL,
        'ARCHIVE_AUTH_HEADER': settings.ARCHIVE_AUTH_HEADER,
        'RAW_DATA_FRAME_URL': settings.RAW_DATA_FRAME_URL,
        'RAW_DATA_AUTH_HEADER': settings.RAW_DATA_AUTH_HEADER
    })

    # Create and run worker
    worker = DownloadWorker(db_address, cache_root, runtime_context)

    try:
        worker.run(poll_interval=poll_interval)
    except KeyboardInterrupt:
        logger.info("Download worker stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error in download worker: {e}", exc_info=True)
        sys.exit(1)
