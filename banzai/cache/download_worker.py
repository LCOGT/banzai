"""Download worker for calibration file caching. Polls DB, downloads missing
calibrations, deletes stale ones."""
import argparse
import os
import sys
import time

from astropy.io import fits
from sqlalchemy import cast, func, String

from banzai import dbs, logs, settings
from banzai.context import Context
from banzai.utils import date_utils, file_utils, fits_utils

logger = logs.get_logger()
HEARTBEAT_INTERVAL = 600        # seconds
FAILURE_RETRY_SECONDS = 43200   # seconds


def get_calibrations_to_cache(db_address, site_id, instrument_types):
    """Return top 2 calibrations per config via SQL window function.

    Runs one query per calibration type so each type partitions by its own
    criteria from settings.CALIBRATION_SET_CRITERIA.
    """
    results = []
    with dbs.get_session(db_address) as session:
        for cal_type, criteria in settings.CALIBRATION_SET_CRITERIA.items():
            partition_cols = [dbs.CalibrationImage.instrument_id]
            for key in criteria:
                partition_cols.append(cast(dbs.CalibrationImage.attributes[key], String))

            rank = func.row_number().over(
                partition_by=partition_cols,
                order_by=dbs.CalibrationImage.dateobs.desc()
            ).label('rank')

            query = session.query(
                dbs.CalibrationImage.id, dbs.CalibrationImage.filename,
                dbs.CalibrationImage.frameid, dbs.CalibrationImage.type,
                dbs.CalibrationImage.dateobs, dbs.CalibrationImage.filepath,
                dbs.Instrument.site.label('site'), dbs.Instrument.camera.label('camera'),
                rank,
            ).join(dbs.Instrument).filter(
                dbs.CalibrationImage.type == cal_type,
                dbs.CalibrationImage.is_master == True,
                dbs.CalibrationImage.is_bad == False,
                dbs.Instrument.site == site_id,
            )
            if instrument_types != ['*']:
                query = query.filter(dbs.Instrument.type.in_(instrument_types))

            subq = query.subquery()
            results.extend(session.query(subq).filter(subq.c.rank <= 2).all())
    return results


def get_cache_path(processed_path, cal):
    epoch = date_utils.epoch_date_to_string(cal.dateobs.date())
    return file_utils.get_processed_path(processed_path, cal.site, cal.camera, epoch)


def update_filepath(db_address, cal_id, filepath):
    with dbs.get_session(db_address) as session:
        cal = session.query(dbs.CalibrationImage).get(cal_id)
        if cal:
            cal.filepath = filepath


def download_calibration(db_address, processed_path, runtime_context, cal):
    """Download file, validate FITS, write to disk, update DB filepath."""
    dest_dir = get_cache_path(processed_path, cal)
    local_path = os.path.join(dest_dir, cal.filename)

    if os.path.exists(local_path):
        logger.info(f"Already on disk: {cal.filename}, updating DB filepath")
        update_filepath(db_address, cal.id, dest_dir)
        return
    if cal.frameid is None:
        logger.warning(f"Skipping {cal.filename} - NULL frameid")
        return

    os.makedirs(dest_dir, exist_ok=True)
    buffer = fits_utils.download_from_s3(
        {'frameid': cal.frameid, 'filename': cal.filename},
        runtime_context, is_raw_frame=False, log_attempts=True,
    )

    try:
        hdulist = fits.open(buffer)
        hdulist.writeto(local_path)
        hdulist.close()
    finally:
        buffer.close()
    update_filepath(db_address, cal.id, dest_dir)
    logger.info(f"Cached {cal.filename}")


def delete_calibration(db_address, cal):
    """Remove file from disk and clear filepath in DB."""
    file_path = os.path.join(cal.filepath, cal.filename)
    if os.path.exists(file_path):
        os.remove(file_path)
        logger.info(f"Deleted {cal.filename}")
    update_filepath(db_address, cal.id, None)


def get_cached_calibrations(db_address, site_id, processed_path):
    """Return all calibrations with a local filepath at this site."""
    with dbs.get_session(db_address) as session:
        return session.query(
            dbs.CalibrationImage.id, dbs.CalibrationImage.filename,
            dbs.CalibrationImage.filepath,
        ).join(dbs.Instrument).filter(
            dbs.CalibrationImage.filepath.isnot(None),
            dbs.CalibrationImage.filepath.like(processed_path + '%'),
            dbs.Instrument.site == site_id,
        ).all()


def run_download_worker(db_address, site_id, instrument_types, processed_path,
                        runtime_context, poll_interval=10):
    """Main loop: poll DB, download missing files, delete stale ones."""
    logger.info("Download worker started")
    failed_frameids: dict[int, float] = {}
    # Start at 0.0 so the first poll always logs a status line on worker startup.
    last_status_log = 0.0

    while True:
        try:
            needed = get_calibrations_to_cache(db_address, site_id, instrument_types)
            needed_filenames = {cal.filename for cal in needed}

            to_download = [cal for cal in needed
                           if not os.path.exists(os.path.join(
                               get_cache_path(processed_path, cal), cal.filename))]

            cached_in_db = get_cached_calibrations(db_address, site_id, processed_path)
            to_delete = [c for c in cached_in_db if c.filename not in needed_filenames]

            now = time.monotonic()
            failed_frameids = {fid: t for fid, t in failed_frameids.items()
                               if now - t < FAILURE_RETRY_SECONDS}
            fresh_to_download = [c for c in to_download if c.frameid not in failed_frameids]

            if fresh_to_download or to_delete or (now - last_status_log >= HEARTBEAT_INTERVAL):
                cached_count = len(needed) - len(to_download)
                logger.info(
                    f"Cache status: {len(needed)} needed, {cached_count} cached, "
                    f"{len(fresh_to_download)} to download, {len(failed_frameids)} failing, "
                    f"{len(to_delete)} to delete"
                )
                last_status_log = now

            downloaded_count = 0
            failed_count = 0
            for cal in fresh_to_download:
                try:
                    download_calibration(db_address, processed_path, runtime_context, cal)
                    downloaded_count += 1
                except Exception as e:
                    failed_frameids[cal.frameid] = time.monotonic()
                    failed_count += 1
                    logger.error(f"Failed to download {cal.filename}: {e}", exc_info=True)

            deleted_count = 0
            for cal in to_delete:
                try:
                    delete_calibration(db_address, cal)
                    deleted_count += 1
                except Exception as e:
                    logger.error(f"Failed to delete {cal.filename}: {e}", exc_info=True)

            if downloaded_count or failed_count or deleted_count:
                logger.info(
                    f"Cache sync done: {downloaded_count} downloaded, "
                    f"{failed_count} failed, {deleted_count} deleted"
                )

            time.sleep(poll_interval)
        except Exception as e:
            logger.error(f"Error in worker loop: {e}", exc_info=True)
            time.sleep(30)


def create_parser():
    parser = argparse.ArgumentParser(description='Run the calibration download worker.')
    parser.add_argument('--db-address', dest='db_address', required=True,
                        help='Database connection string')
    parser.add_argument('--site-id', dest='site_id', required=True,
                        help='Site identifier (e.g. lsc, ogg)')
    parser.add_argument('--instrument-types', dest='instrument_types', default='*',
                        help='Comma-separated instrument types, or * for all (default: *)')
    parser.add_argument('--processed-path', dest='processed_path', default='/calibrations',
                        help='Path for cached calibration files (default: /calibrations)')
    parser.add_argument('--poll-interval', dest='poll_interval', type=int, default=10,
                        help='Seconds between poll cycles (default: 10)')
    return parser


def run_download_worker_daemon():
    """Entry point: parse args, start worker loop."""
    args = create_parser().parse_args()

    instrument_types = ([t.strip() for t in args.instrument_types.split(',')]
                        if args.instrument_types != '*' else ['*'])
    runtime_context = Context(settings)

    try:
        run_download_worker(args.db_address, args.site_id, instrument_types, args.processed_path,
                            runtime_context, args.poll_interval)
    except KeyboardInterrupt:
        logger.info("Download worker stopped")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
