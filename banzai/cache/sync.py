#!/usr/bin/env python
"""
Sync calibration data from remote database to local cache.
"""
import os
import argparse
import datetime
from astropy.io import fits
from banzai import dbs
from banzai.logs import get_logger
from banzai.utils import fits_utils
import banzai.settings as settings
from banzai.exceptions import FrameNotAvailableError
from sqlalchemy import text

logger = get_logger()


class MinimalContext:
    """Minimal context for downloading files from S3"""
    def __init__(self):
        self.ARCHIVE_FRAME_URL = settings.ARCHIVE_FRAME_URL
        self.ARCHIVE_AUTH_HEADER = settings.ARCHIVE_AUTH_HEADER
        self.RAW_DATA_FRAME_URL = settings.RAW_DATA_FRAME_URL
        self.RAW_DATA_AUTH_HEADER = settings.RAW_DATA_AUTH_HEADER


def verify_fits_file(filepath):
    """
    Quick check if a file is a valid FITS file.
    Returns True if valid, False otherwise.
    """
    try:
        with fits.open(filepath, memmap=False) as hdul:
            # Just accessing the header verifies basic FITS structure
            _ = hdul[0].header
            # Check it has at least one HDU
            if len(hdul) == 0:
                return False
        return True
    except Exception as e:
        logger.warning(f"FITS validation failed for {filepath}: {e}")
        return False


def sync_calibrations(remote_db_address, cache_db_address, cache_file_root,
                     site_id, runtime_context=None):
    """
    Sync calibration records and files from remote to cache.

    Parameters
    ----------
    remote_db_address : str
        PostgreSQL database address
    cache_db_address : str
        SQLite cache database address
    cache_file_root : str
        Root directory for cached files
    site_id : str
        Site code to sync
    runtime_context : RuntimeContext, optional
        Runtime context for downloading files. If None, creates MinimalContext.
    """
    if runtime_context is None:
        runtime_context = MinimalContext()

    logger.info(f"Starting sync for site {site_id}")

    # First pass: collect all records and count what needs downloading
    files_to_download = []
    total_records = 0
    thirty_days_ago = datetime.datetime.now() - datetime.timedelta(days=30)

    instruments_with_calibrations = set()  # Track which instruments have regular calibrations

    with dbs.get_session(remote_db_address) as db_session:
        instruments = dbs.get_instruments_at_site(site_id, remote_db_address)

        for instrument in instruments:
            logger.info(f"Scanning calibrations for instrument {instrument.name}")

            # Phase 1: Query for regular calibrations (excluding READNOISE and BPM) with 30-day age limit
            # This ensures we only get the latest calibration for each unique combination
            # Different calibration types use different grouping criteria:
            # - BIAS/DARK: grouped by (instrument_id, type, configuration_mode, binning)
            # - SKYFLAT/FLAT: grouped by (instrument_id, type, configuration_mode, binning, filter)
            sql_query_regular = text("""
                SELECT DISTINCT ON (c.instrument_id, c.type,
                                    c.attributes->>'configuration_mode',
                                    c.attributes->>'binning',
                                    CASE WHEN c.type IN ('SKYFLAT', 'FLAT')
                                        THEN c.attributes->>'filter'
                                        ELSE NULL END)
                    c.id, c.type, c.filename, c.filepath, c.frameid, c.dateobs, c.datecreated,
                    c.instrument_id, c.is_master, c.is_bad, c.good_until, c.good_after, c.attributes
                FROM calimages c
                WHERE c.instrument_id = :instrument_id
                    AND c.is_master = true
                    AND c.is_bad = false
                    AND c.type NOT IN ('READNOISE', 'BPM')
                    AND c.dateobs >= :min_date
                ORDER BY c.instrument_id, c.type,
                            c.attributes->>'configuration_mode',
                            c.attributes->>'binning',
                            CASE WHEN c.type IN ('SKYFLAT', 'FLAT')
                                THEN c.attributes->>'filter'
                                ELSE NULL END,
                            c.dateobs DESC
            """)

            result = db_session.execute(sql_query_regular, {
                'instrument_id': instrument.id,
                'min_date': thirty_days_ago
            })

            records = []
            for row in result:
                # Convert row to CalibrationImage object
                record = dbs.CalibrationImage(
                    id=row.id,
                    type=row.type,
                    filename=row.filename,
                    filepath=row.filepath,
                    frameid=row.frameid,
                    dateobs=row.dateobs,
                    datecreated=row.datecreated,
                    instrument_id=row.instrument_id,
                    is_master=row.is_master,
                    is_bad=row.is_bad,
                    good_until=row.good_until,
                    good_after=row.good_after,
                    attributes=row.attributes
                )
                records.append(record)

            # If we found regular calibrations, mark this instrument and query for READNOISE/BPM
            if records:
                instruments_with_calibrations.add(instrument.id)

                # Phase 2: Query for READNOISE and BPM without age limit
                sql_query_readnoise_bpm = text("""
                    SELECT DISTINCT ON (c.instrument_id, c.type,
                                        c.attributes->>'configuration_mode',
                                        c.attributes->>'binning')
                        c.id, c.type, c.filename, c.filepath, c.frameid, c.dateobs, c.datecreated,
                        c.instrument_id, c.is_master, c.is_bad, c.good_until, c.good_after, c.attributes
                    FROM calimages c
                    WHERE c.instrument_id = :instrument_id
                        AND c.is_master = true
                        AND c.is_bad = false
                        AND c.type IN ('READNOISE', 'BPM')
                    ORDER BY c.instrument_id, c.type,
                                c.attributes->>'configuration_mode',
                                c.attributes->>'binning',
                                c.dateobs DESC
                """)

                result_readnoise_bpm = db_session.execute(sql_query_readnoise_bpm, {
                    'instrument_id': instrument.id
                })

                for row in result_readnoise_bpm:
                    # Convert row to CalibrationImage object
                    record = dbs.CalibrationImage(
                        id=row.id,
                        type=row.type,
                        filename=row.filename,
                        filepath=row.filepath,
                        frameid=row.frameid,
                        dateobs=row.dateobs,
                        datecreated=row.datecreated,
                        instrument_id=row.instrument_id,
                        is_master=row.is_master,
                        is_bad=row.is_bad,
                        good_until=row.good_until,
                        good_after=row.good_after,
                        attributes=row.attributes
                    )
                    records.append(record)

            total_records += len(records)

            for record in records:
                # Skip records with NULL frameid - cannot download without frame ID
                if record.frameid is None:
                    logger.warning(f"Skipping {record.filename} (type: {record.type}) - NULL frameid in database")
                    continue

                cal_type = record.type
                local_dir = os.path.join(cache_file_root, cal_type)
                local_path = os.path.join(local_dir, record.filename)

                # Ensure directory exists
                os.makedirs(local_dir, exist_ok=True)

                # Check if file exists and validate it
                if os.path.exists(local_path):
                    # Verify the cached file is valid FITS
                    if not verify_fits_file(local_path):
                        logger.warning(f"Removing corrupt cached file: {record.filename}")
                        try:
                            os.remove(local_path)
                        except Exception as e:
                            logger.error(f"Could not remove corrupt file {local_path}: {e}")
                            continue
                    else:
                        # File exists and is valid, skip download
                        continue

                # File doesn't exist or was corrupt (and removed), queue for download
                files_to_download.append((record, local_dir, local_path))

    # Report what will be downloaded
    logger.info(f"Found {total_records} total calibration records")
    logger.info(f"Need to download {len(files_to_download)} files")
    logger.info(f"Already cached: {total_records - len(files_to_download)} files")

    if len(files_to_download) == 0:
        logger.info("Cache is up to date, nothing to download")
    else:
        # Second pass: download files with progress
        total_downloaded = 0

        for i, (record, local_dir, local_path) in enumerate(files_to_download, 1):
            logger.info(f"Downloading {i}/{len(files_to_download)}: {record.filename}")
            try:
                file_info = {'frameid': record.frameid, 'filename': record.filename}
                buffer = fits_utils.download_from_s3(file_info, runtime_context, is_raw_frame=False)

                with open(local_path, 'wb') as f:
                    f.write(buffer.read())
                buffer.close()

                # Validate downloaded file immediately
                if not verify_fits_file(local_path):
                    logger.warning(f"Downloaded file {record.filename} failed FITS validation, removing...")
                    try:
                        os.remove(local_path)
                    except Exception as e:
                        logger.error(f"Could not remove corrupt downloaded file {local_path}: {e}")
                    continue

                total_downloaded += 1

            except FrameNotAvailableError as e:
                logger.warning(f"Skipping {record.filename} (frameid: {record.frameid}, type: {record.type}): "
                              f"Frame not available in archive")
                continue
            except Exception as e:
                logger.error(f"Failed to download {record.filename}: {e}")
                continue

        logger.info(f"Downloaded {total_downloaded}/{len(files_to_download)} files successfully")

    # Third pass: update all records in cache database (both downloaded and existing)
    logger.info("Updating cache database records...")
    total_synced = 0
    synced_filenames = set()  # Track all synced filenames for cleanup

    with dbs.get_session(remote_db_address) as db_session:
        instruments = dbs.get_instruments_at_site(site_id, remote_db_address)

        for instrument in instruments:
            # Phase 1: Query for regular calibrations (excluding READNOISE and BPM) with 30-day age limit
            sql_query_regular = text("""
                SELECT DISTINCT ON (c.instrument_id, c.type,
                                   c.attributes->>'configuration_mode',
                                   c.attributes->>'binning',
                                   CASE WHEN c.type IN ('SKYFLAT', 'FLAT')
                                        THEN c.attributes->>'filter'
                                        ELSE NULL END)
                    c.id, c.type, c.filename, c.filepath, c.frameid, c.dateobs, c.datecreated,
                    c.instrument_id, c.is_master, c.is_bad, c.good_until, c.good_after, c.attributes
                FROM calimages c
                WHERE c.instrument_id = :instrument_id
                  AND c.is_master = true
                  AND c.is_bad = false
                  AND c.type NOT IN ('READNOISE', 'BPM')
                  AND c.dateobs >= :min_date
                ORDER BY c.instrument_id, c.type,
                         c.attributes->>'configuration_mode',
                         c.attributes->>'binning',
                         CASE WHEN c.type IN ('SKYFLAT', 'FLAT')
                              THEN c.attributes->>'filter'
                              ELSE NULL END,
                         c.dateobs DESC
            """)

            result = db_session.execute(sql_query_regular, {
                'instrument_id': instrument.id,
                'min_date': thirty_days_ago
            })

            records = []
            for row in result:
                # Convert row to CalibrationImage object
                record = dbs.CalibrationImage(
                    id=row.id,
                    type=row.type,
                    filename=row.filename,
                    filepath=row.filepath,
                    frameid=row.frameid,
                    dateobs=row.dateobs,
                    datecreated=row.datecreated,
                    instrument_id=row.instrument_id,
                    is_master=row.is_master,
                    is_bad=row.is_bad,
                    good_until=row.good_until,
                    good_after=row.good_after,
                    attributes=row.attributes
                )
                records.append(record)

            # If we found regular calibrations, also query for READNOISE/BPM
            if records:
                # Phase 2: Query for READNOISE and BPM without age limit
                sql_query_readnoise_bpm = text("""
                    SELECT DISTINCT ON (c.instrument_id, c.type,
                                       c.attributes->>'configuration_mode',
                                       c.attributes->>'binning')
                        c.id, c.type, c.filename, c.filepath, c.frameid, c.dateobs, c.datecreated,
                        c.instrument_id, c.is_master, c.is_bad, c.good_until, c.good_after, c.attributes
                    FROM calimages c
                    WHERE c.instrument_id = :instrument_id
                      AND c.is_master = true
                      AND c.is_bad = false
                      AND c.type IN ('READNOISE', 'BPM')
                    ORDER BY c.instrument_id, c.type,
                             c.attributes->>'configuration_mode',
                             c.attributes->>'binning',
                             c.dateobs DESC
                """)

                result_readnoise_bpm = db_session.execute(sql_query_readnoise_bpm, {
                    'instrument_id': instrument.id
                })

                for row in result_readnoise_bpm:
                    # Convert row to CalibrationImage object
                    record = dbs.CalibrationImage(
                        id=row.id,
                        type=row.type,
                        filename=row.filename,
                        filepath=row.filepath,
                        frameid=row.frameid,
                        dateobs=row.dateobs,
                        datecreated=row.datecreated,
                        instrument_id=row.instrument_id,
                        is_master=row.is_master,
                        is_bad=row.is_bad,
                        good_until=row.good_until,
                        good_after=row.good_after,
                        attributes=row.attributes
                    )
                    records.append(record)

            for record in records:
                cal_type = record.type
                local_dir = os.path.join(cache_file_root, cal_type)
                local_path = os.path.join(local_dir, record.filename)

                # Only update DB if file exists locally (either was cached or just downloaded)
                if os.path.exists(local_path):
                    # Save to cache database
                    with dbs.get_session(cache_db_address) as cache_session:
                        dbs.add_or_update_record(
                            cache_session,
                            dbs.CalibrationImage,
                            {'filename': record.filename},
                            {
                                'type': record.type,
                                'filename': record.filename,
                                'filepath': local_dir,
                                'frameid': record.frameid,
                                'dateobs': record.dateobs,
                                'datecreated': record.datecreated,
                                'instrument_id': record.instrument_id,
                                'is_master': record.is_master,
                                'is_bad': record.is_bad,
                                'good_until': record.good_until,
                                'good_after': record.good_after,
                                'attributes': record.attributes
                            }
                        )
                        cache_session.commit()

                    total_synced += 1
                    synced_filenames.add(record.filename)

    logger.info(f"Sync complete: {total_synced} records synced to cache database")

    # Fourth pass: cleanup obsolete calibrations
    logger.info("Cleaning up obsolete calibrations...")
    total_obsolete = 0
    files_deleted = 0
    records_deleted = 0
    failed_deletions = []

    with dbs.get_session(cache_db_address) as cache_session:
        instruments = dbs.get_instruments_at_site(site_id, remote_db_address)

        for instrument in instruments:
            # Get all cached records for this instrument
            cached_records = cache_session.query(dbs.CalibrationImage).filter(
                dbs.CalibrationImage.instrument_id == instrument.id
            ).all()

            # Identify obsolete records (not in current sync)
            for cached_record in cached_records:
                if cached_record.filename not in synced_filenames:
                    total_obsolete += 1

                    # Try to delete the file from disk
                    if cached_record.filepath:
                        file_path = os.path.join(cached_record.filepath, cached_record.filename)
                        if os.path.exists(file_path):
                            try:
                                os.remove(file_path)
                                files_deleted += 1
                                logger.info(f"Removed obsolete calibration: {cached_record.filename} (type: {cached_record.type})")
                            except Exception as e:
                                logger.error(f"Failed to delete file {cached_record.filename}: {e}")
                                failed_deletions.append(cached_record.filename)
                                continue

                    # Delete the database record
                    cache_session.delete(cached_record)
                    records_deleted += 1

        cache_session.commit()

    # Log cleanup statistics
    logger.info(f"Found {total_obsolete} obsolete calibration records")
    logger.info(f"Deleted {files_deleted} files from disk")
    logger.info(f"Removed {records_deleted} records from cache database")
    if failed_deletions:
        logger.warning(f"Failed to delete {len(failed_deletions)} files: {', '.join(failed_deletions)}")
    logger.info("Cleanup complete")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sync calibration cache')
    parser.add_argument('--remote-db', required=True,
                       help='Remote PostgreSQL database address')
    parser.add_argument('--cache-db', required=True,
                       help='Local SQLite cache database address')
    parser.add_argument('--cache-root', required=True,
                       help='Root directory for cached files')
    parser.add_argument('--site', required=True,
                       help='Site code to sync')

    args = parser.parse_args()

    # Create runtime context for downloading
    runtime_context = MinimalContext()

    # Sync calibrations
    sync_calibrations(args.remote_db, args.cache_db, args.cache_root,
                     args.site, runtime_context)
