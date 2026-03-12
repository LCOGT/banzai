"""Download worker for calibration file caching. Polls DB, downloads missing
calibrations, deletes stale ones."""
import os
import sys
import time

from astropy.io import fits
from sqlalchemy import cast, func, String

from banzai import dbs, logs, settings
from banzai.context import Context
from banzai.utils import date_utils, file_utils, fits_utils

logger = logs.get_logger()
HEARTBEAT_INTERVAL = 300


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


def _update_filepath(db_address, cal_id, filepath):
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
        _update_filepath(db_address, cal.id, dest_dir)
        return
    if cal.frameid is None:
        logger.warning(f"Skipping {cal.filename} - NULL frameid")
        return

    os.makedirs(dest_dir, exist_ok=True)
    logger.info(f"Downloading {cal.filename} (frameid={cal.frameid})")
    buffer = fits_utils.download_from_s3(
        {'frameid': cal.frameid, 'filename': cal.filename},
        runtime_context, is_raw_frame=False
    )

    try:
        hdulist = fits.open(buffer)
        hdulist.writeto(local_path)
        hdulist.close()
    finally:
        buffer.close()
    _update_filepath(db_address, cal.id, dest_dir)
    logger.info(f"Downloaded {cal.filename}")


def delete_calibration(db_address, cal):
    """Remove file from disk and clear filepath in DB."""
    file_path = os.path.join(cal.filepath, cal.filename)
    if os.path.exists(file_path):
        os.remove(file_path)
        logger.info(f"Deleted {cal.filename}")
    _update_filepath(db_address, cal.id, None)


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
    last_heartbeat = time.monotonic()

    while True:
        try:
            needed = get_calibrations_to_cache(db_address, site_id, instrument_types)
            needed_filenames = {cal.filename for cal in needed}

            # Download calibrations not yet on local disk
            to_download = [cal for cal in needed
                           if not os.path.exists(os.path.join(
                               get_cache_path(processed_path, cal), cal.filename))]

            # Find locally-cached cals no longer in the top-2 needed set
            cached_in_db = get_cached_calibrations(db_address, site_id, processed_path)
            to_delete = [c for c in cached_in_db if c.filename not in needed_filenames]

            for cal in to_download:
                try:
                    download_calibration(db_address, processed_path, runtime_context, cal)
                except Exception as e:
                    logger.error(f"Failed to download {cal.filename}: {e}", exc_info=True)
            for cal in to_delete:
                try:
                    delete_calibration(db_address, cal)
                except Exception as e:
                    logger.error(f"Failed to delete {cal.filename}: {e}", exc_info=True)

            now = time.monotonic()
            if to_download or to_delete:
                logger.info(f"Cache sync: downloaded {len(to_download)}, "
                            f"deleted {len(to_delete)}, total needed {len(needed)}")
                last_heartbeat = now
            elif now - last_heartbeat >= HEARTBEAT_INTERVAL:
                logger.info(f"Cache healthy: {len(needed)} calibrations tracked")
                last_heartbeat = now

            time.sleep(poll_interval)
        except Exception as e:
            logger.error(f"Error in worker loop: {e}", exc_info=True)
            time.sleep(30)


def run_download_worker_daemon():
    """Entry point: read env vars, start worker loop."""
    db_address = os.getenv('DB_ADDRESS')
    site_id = os.getenv('SITE_ID')
    instrument_types_str = os.getenv('INSTRUMENT_TYPES', '*')
    processed_path = os.getenv('PROCESSED_PATH', '/data/processed')
    poll_interval = int(os.getenv('DOWNLOAD_WORKER_POLL_INTERVAL', '10'))

    if not db_address or not site_id:
        logger.error('DB_ADDRESS and SITE_ID environment variables are required')
        sys.exit(1)

    instrument_types = ([t.strip() for t in instrument_types_str.split(',')]
                        if instrument_types_str != '*' else ['*'])
    runtime_context = Context(settings)

    try:
        run_download_worker(db_address, site_id, instrument_types, processed_path,
                            runtime_context, poll_interval)
    except KeyboardInterrupt:
        logger.info("Download worker stopped")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
