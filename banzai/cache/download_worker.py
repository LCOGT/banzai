"""
Download worker for calibration file caching.

This module implements a worker-driven download system that queries the database
directly to determine which calibration files should be cached. The worker polls
the database periodically, compares needed files with cached files, and downloads
or deletes files as necessary.

Part of the PostgreSQL replication-based calibration cache system.
"""

import os
import sys
import time

from astropy.io import fits
from sqlalchemy import text

from banzai import dbs, logs
from banzai.utils import fits_utils
from banzai.exceptions import FrameNotAvailableError

logger = logs.get_logger()

# Heartbeat interval for idle cache status logging (5 minutes)
HEARTBEAT_INTERVAL = 300


class DownloadWorker:
    """
    Worker-driven calibration file download system.

    Queries the database directly to determine which calibration files should
    be cached, compares with filesystem state, and downloads/deletes files
    as needed. Uses a flat directory structure with all calibration files
    stored in a single directory.

    Attributes:
        db_address: Database connection string for local site database
        cache_root: Root directory for calibration file storage (flat structure)
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

        # Logging state tracking
        self._last_needed_count = None
        self._last_cached_count = None
        self._last_summary_time = 0
        self._startup = True

        logger.info(f"Initialized download worker with cache_root: {cache_root}")

    def get_cache_config(self):
        """
        Get cache configuration from database.

        Returns:
            CacheConfig object with site_id and instrument_types, or None if not configured
        """
        with dbs.get_session(self.db_address) as session:
            config = session.query(dbs.CacheConfig).first()
        return config

    def get_calibrations_to_cache(self):
        """
        Query for top 2 versions of each calibration configuration.

        Executes a ranking query to find the 2 most recent calibrations for each
        unique combination of (instrument, type, config_mode, binning, filter).
        Only returns calibrations matching the site's filter configuration.

        Returns:
            dict: {filename: CalibrationImage} mapping filenames to database records.
                  Each CalibrationImage has: id, filename, type, frameid, instrument_id,
                  attributes (JSON with config_mode, binning, filter)
        """
        config = self.get_cache_config()
        if not config:
            logger.warning("No cache configuration found - cache_config table may not be initialized")
            return {}

        # SQL query to rank calibrations and select top 2 per configuration
        ranking_sql = """
        WITH ranked_cals AS (
            SELECT
                c.id,
                c.filename,
                c.type,
                c.instrument_id,
                c.filepath,
                c.frameid,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        c.instrument_id,
                        c.type,
                        c.attributes->>'configuration_mode',
                        c.attributes->>'binning',
                        CASE WHEN c.type IN ('SKYFLAT', 'FLAT')
                            THEN c.attributes->>'filter'
                            ELSE NULL
                        END
                    ORDER BY c.dateobs DESC, c.id DESC
                ) as version_rank
            FROM calimages c
            JOIN instruments i ON c.instrument_id = i.id
            WHERE
                c.is_master = true
                AND c.is_bad = false
                AND i.site = :site_id
                AND (
                    :instrument_types = ARRAY['*']
                    OR i.type = ANY(:instrument_types)
                )
        )
        SELECT id, filename, type, filepath, frameid
        FROM ranked_cals
        WHERE version_rank <= 2;
        """

        try:
            with dbs.get_session(self.db_address) as session:
                # Execute query with parameters
                results = session.execute(
                    text(ranking_sql),
                    {
                        'site_id': config.site_id,
                        'instrument_types': config.instrument_types
                    }
                ).fetchall()

                # Build dict of filename -> CalibrationImage
                needed = {}
                for row in results:
                    cal = session.query(dbs.CalibrationImage).get(row.id)
                    if cal:
                        needed[cal.filename] = cal

                logger.debug(f"Found {len(needed)} calibrations that should be cached")
                return needed

        except Exception as e:
            logger.error(f"Error querying calibrations to cache: {e}", exc_info=True)
            return {}

    def get_cached_files(self):
        """
        Scan filesystem and return set of filenames currently on disk.

        Returns:
            set: Filenames found in cache directory
        """
        if os.path.exists(self.cache_root):
            try:
                files = set(os.listdir(self.cache_root))
                # Filter out any non-FITS files or temp files
                files = {f for f in files if f.endswith('.fits') or f.endswith('.fits.fz')}
                logger.debug(f"Found {len(files)} files in cache directory")
                return files
            except Exception as e:
                logger.error(f"Error scanning cache directory: {e}", exc_info=True)
                return set()
        else:
            logger.info(f"Cache directory does not exist yet: {self.cache_root}")
            return set()

    def log_calibration_summary(self, needed_cals, log_details=False):
        """
        Log summary of calibration configurations.

        Args:
            needed_cals: Dict of {filename: CalibrationImage} for needed calibrations
            log_details: If True, log detailed per-configuration breakdown
        """
        if not needed_cals:
            return

        # Count by type
        type_counts = {}
        for cal in needed_cals.values():
            type_counts[cal.type] = type_counts.get(cal.type, 0) + 1

        # Log summary
        logger.info(f"Calibration summary ({len(needed_cals)} total files):")
        for cal_type in sorted(type_counts.keys()):
            logger.info(f"  {cal_type}: {type_counts[cal_type]} files")

        # Log detailed breakdown if requested
        if log_details:
            logger.info("Configuration breakdown:")
            # Group by configuration
            configs = {}
            for cal in needed_cals.values():
                # Build config key
                instrument_id = cal.instrument_id
                cal_type = cal.type
                config_mode = cal.attributes.get('configuration_mode', 'unknown')
                binning = cal.attributes.get('binning', 'unknown')
                filter_name = cal.attributes.get('filter', '') if cal_type in ('SKYFLAT', 'FLAT') else ''

                if filter_name:
                    config_key = f"  instrument_{instrument_id}/{cal_type}/{config_mode}/{binning}/{filter_name}"
                else:
                    config_key = f"  instrument_{instrument_id}/{cal_type}/{config_mode}/{binning}"

                configs[config_key] = configs.get(config_key, 0) + 1

            for config_key in sorted(configs.keys()):
                logger.info(f"{config_key}: {configs[config_key]} file(s)")

    def _get_type_summary(self, needed_cals):
        """Build compact type summary string like 'TYPE:count, TYPE:count, ...'."""
        if not needed_cals:
            return "empty"
        type_counts = {}
        for cal in needed_cals.values():
            type_counts[cal.type] = type_counts.get(cal.type, 0) + 1
        return ", ".join(f"{t}:{c}" for t, c in sorted(type_counts.items()))

    def reconcile_orphaned_filepaths(self, needed_filenames):
        """
        Clear filepath for database rows that shouldn't be cached.

        Finds calibrations with filepath set to our cache_root but whose
        filename is not in the set of files that should be cached. This
        handles cases where filepath was incorrectly set or where the file
        was deleted outside the worker's control.

        Args:
            needed_filenames: Set of filenames that should be cached
        """
        with dbs.get_session(self.db_address) as session:
            # Set application_name so trigger allows the update
            session.execute(text("SET LOCAL application_name = 'banzai_download_worker'"))

            # Find rows with filepath pointing to our cache_root
            # but not in the needed set
            cals_with_filepath = session.query(dbs.CalibrationImage).filter(
                dbs.CalibrationImage.filepath == self.cache_root
            ).all()

            orphaned_count = 0
            for cal in cals_with_filepath:
                if cal.filename not in needed_filenames:
                    logger.info(f"Clearing orphaned filepath for {cal.filename} "
                               f"(filepath set but not in needed cache set)")
                    cal.filepath = None
                    orphaned_count += 1

            if orphaned_count > 0:
                session.commit()
                logger.info(f"Cleared {orphaned_count} orphaned filepath(s)")

    def safe_to_delete(self, filename, needed_cals, files_on_disk):
        """
        Check if it's safe to delete this calibration file.

        A file is only safe to delete if we have 2+ OTHER files for the same
        configuration already on disk. This prevents deleting the only working
        calibration if a download fails.

        Args:
            filename: Filename of the calibration to potentially delete
            needed_cals: Dict of {filename: CalibrationImage} for all needed calibrations
            files_on_disk: Set of filenames currently cached

        Returns:
            bool: True if safe to delete, False otherwise
        """
        # Get the calibration info for the file we want to delete
        # It's not in needed_cals (that's why we want to delete it), so we need to query it
        with dbs.get_session(self.db_address) as session:
            cal_to_delete = session.query(dbs.CalibrationImage)\
                .filter_by(filename=filename).first()

            if not cal_to_delete:
                logger.warning(f"Cannot find calibration record for {filename}, skipping deletion")
                return False

            # Find all calibrations for the same configuration in needed_cals
            same_config = []
            for needed_cal in needed_cals.values():
                # Check if this calibration is the same configuration
                if (needed_cal.instrument_id == cal_to_delete.instrument_id and
                    needed_cal.type == cal_to_delete.type and
                    needed_cal.attributes.get('configuration_mode') == cal_to_delete.attributes.get('configuration_mode') and
                    needed_cal.attributes.get('binning') == cal_to_delete.attributes.get('binning')):

                    # For SKYFLAT/FLAT types, also check filter
                    if cal_to_delete.type in ('SKYFLAT', 'FLAT'):
                        if needed_cal.attributes.get('filter') == cal_to_delete.attributes.get('filter'):
                            same_config.append(needed_cal)
                    else:
                        same_config.append(needed_cal)

            # Count how many of these are actually on disk
            files_for_config_on_disk = [
                cal for cal in same_config
                if cal.filename in files_on_disk
            ]

            # Safe to delete if we have 2+ other files on disk
            return len(files_for_config_on_disk) >= 2

    def run(self, poll_interval=10):
        """
        Main worker loop - query-based approach.

        Continuously polls the database to determine which files should be cached,
        compares with filesystem state, and downloads/deletes files as needed.

        Args:
            poll_interval: Seconds to wait between polls (default: 10)
        """
        logger.info("Starting download worker")

        while True:
            try:
                # What SHOULD be cached (from database)
                needed = self.get_calibrations_to_cache()  # {filename: CalibrationImage}

                # What IS cached (from filesystem)
                cached = self.get_cached_files()  # set of filenames

                # Determine what's changed
                needed_count = len(needed)
                cached_count = len(cached)
                to_download = set(needed.keys()) - cached
                to_delete = cached - set(needed.keys())
                now = time.time()

                # Smart logging based on state
                if self._startup:
                    # Startup: full summary
                    logger.info(f"Initial cache state: {cached_count}/{needed_count} files cached")
                    self.log_calibration_summary(needed, log_details=False)
                    self._startup = False
                    self._last_summary_time = now
                elif to_download or to_delete:
                    # Downloads/deletes needed: full summary + details
                    logger.info(f"Cache sync needed: {len(to_download)} to download, "
                                f"{len(to_delete)} to delete")
                    self.log_calibration_summary(needed, log_details=True)
                    self._last_summary_time = now
                elif (needed_count != self._last_needed_count or
                      cached_count != self._last_cached_count):
                    # State changed (counts differ): brief update
                    logger.info(f"Cache state changed: {cached_count}/{needed_count} files cached "
                                f"(was {self._last_cached_count}/{self._last_needed_count})")
                    self._last_summary_time = now
                elif now - self._last_summary_time >= HEARTBEAT_INTERVAL:
                    # Heartbeat (5 min since last summary): single line with type breakdown
                    type_summary = self._get_type_summary(needed)
                    logger.info(f"Cache healthy: {cached_count}/{needed_count} files cached "
                                f"({type_summary})")
                    self._last_summary_time = now

                # Update tracking state
                self._last_needed_count = needed_count
                self._last_cached_count = cached_count

                # Download missing files
                if to_download:
                    for filename in to_download:
                        cal_info = needed[filename]
                        try:
                            self.download_calibration(cal_info)
                        except Exception as e:
                            logger.error(f"Failed to download {filename}: {e}", exc_info=True)

                # Delete outdated files (with safety check)
                if to_delete:
                    for filename in to_delete:
                        if self.safe_to_delete(filename, needed, cached):
                            try:
                                self.delete_calibration(filename)
                                # Update cached set after successful deletion
                                cached.discard(filename)
                            except Exception as e:
                                logger.error(f"Failed to delete {filename}: {e}", exc_info=True)

                # Clear orphaned filepaths (rows with filepath set but not in needed set)
                self.reconcile_orphaned_filepaths(set(needed.keys()))

                # Sleep before next poll
                time.sleep(poll_interval)

            except KeyboardInterrupt:
                logger.info("Download worker interrupted")
                break
            except Exception as e:
                logger.error(f"Error in worker loop: {e}", exc_info=True)
                time.sleep(30)  # Back off on errors

        logger.info("Download worker stopped")

    def download_calibration(self, cal_info):
        """
        Download a single calibration file and update filepath.

        Downloads the FITS file from the archive, validates it, saves to disk
        atomically (using temp file + rename), and updates the database with
        the local file path.

        Args:
            cal_info: CalibrationImage object with id, filename, frameid, type

        Raises:
            FrameNotAvailableError: If frame cannot be downloaded from archive
            ValueError: If frameid is NULL or downloaded file is invalid
            Exception: For other errors during download
        """
        local_path = os.path.join(self.cache_root, cal_info.filename)

        # Skip if already exists (race condition protection)
        if os.path.exists(local_path):
            logger.info(f"File already exists: {cal_info.filename}, updating database")
            with dbs.get_session(self.db_address) as session:
                cal_record = session.query(dbs.CalibrationImage).get(cal_info.id)
                if cal_record and cal_record.filepath != self.cache_root:
                    cal_record.filepath = self.cache_root
                    session.commit()
            return

        # Check for NULL frameid
        if cal_info.frameid is None:
            logger.warning(f"Skipping {cal_info.filename} (type: {cal_info.type}) - NULL frameid in database")
            return

        # Create cache directory if needed
        os.makedirs(self.cache_root, exist_ok=True)

        # Download from archive
        logger.info(f"Downloading {cal_info.filename} (frameid: {cal_info.frameid}, type: {cal_info.type})")

        file_info = {
            'frameid': cal_info.frameid,
            'filename': cal_info.filename
        }

        # Download file to buffer
        buffer = fits_utils.download_from_s3(
            file_info,
            self.runtime_context,
            is_raw_frame=False
        )

        # Write to temp file first (atomic operation)
        temp_path = local_path + '.tmp'
        try:
            with open(temp_path, 'wb') as f:
                f.write(buffer.read())
            buffer.close()

            # Validate FITS file
            if not self.validate_fits(temp_path):
                os.remove(temp_path)
                raise ValueError(f"Invalid FITS file: {cal_info.filename}")

            # Atomic rename to final location
            os.rename(temp_path, local_path)

            # Update database with filepath
            with dbs.get_session(self.db_address) as session:
                cal_record = session.query(dbs.CalibrationImage).get(cal_info.id)
                if cal_record:
                    cal_record.filepath = self.cache_root
                    session.commit()

            logger.info(f"Successfully downloaded {cal_info.filename}")

        except Exception as e:
            # Clean up temp file if it exists
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def delete_calibration(self, filename):
        """
        Delete a calibration file and clear filepath in database.

        Removes the file from disk and sets filepath=NULL in the database.

        Args:
            filename: Name of the file to delete
        """
        file_path = os.path.join(self.cache_root, filename)

        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Deleted file: {filename}")
            except Exception as e:
                logger.error(f"Failed to delete file {filename}: {e}")
                raise

        # Clear filepath in database
        # Set application_name so trigger allows the update
        with dbs.get_session(self.db_address) as session:
            session.execute(text("SET LOCAL application_name = 'banzai_download_worker'"))
            cal = session.query(dbs.CalibrationImage)\
                .filter_by(filename=filename).first()
            if cal:
                cal.filepath = None
                session.commit()
                logger.info(f"Cleared filepath in database for {filename}")

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
