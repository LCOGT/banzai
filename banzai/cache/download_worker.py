"""Download worker for calibration file caching. Polls DB, downloads missing
calibrations, deletes stale ones."""
import os
import sys
import time

from sqlalchemy import cast, func, String

from banzai import dbs, logs, settings
from banzai.context import Context
from banzai.utils import date_utils, fits_utils

logger = logs.get_logger()
HEARTBEAT_INTERVAL = 300


class DownloadWorker:
    def __init__(self, db_address, site_id, instrument_types, processed_path, runtime_context):
        self.db_address = db_address
        self.site_id = site_id
        self.instrument_types = instrument_types
        self.processed_path = processed_path
        self.runtime_context = runtime_context

    def get_calibrations_to_cache(self):
        """Return top 2 calibrations per config via SQL window function."""
        with dbs.get_session(self.db_address) as session:
            config_mode = cast(dbs.CalibrationImage.attributes['configuration_mode'], String)
            binning = cast(dbs.CalibrationImage.attributes['binning'], String)
            filter_col = cast(dbs.CalibrationImage.attributes['filter'], String)

            rank = func.row_number().over(
                partition_by=[dbs.CalibrationImage.instrument_id, dbs.CalibrationImage.type,
                              config_mode, binning, filter_col],
                order_by=dbs.CalibrationImage.dateobs.desc()
            ).label('rank')

            query = session.query(
                dbs.CalibrationImage.id, dbs.CalibrationImage.filename,
                dbs.CalibrationImage.frameid, dbs.CalibrationImage.type,
                dbs.CalibrationImage.dateobs, dbs.CalibrationImage.filepath,
                dbs.Instrument.site.label('site'), dbs.Instrument.camera.label('camera'),
                rank,
            ).join(dbs.Instrument).filter(
                dbs.CalibrationImage.is_master == True,
                dbs.CalibrationImage.is_bad == False,
                dbs.Instrument.site == self.site_id,
            )
            if self.instrument_types != ['*']:
                query = query.filter(dbs.Instrument.type.in_(self.instrument_types))

            subq = query.subquery()
            return session.query(subq).filter(subq.c.rank <= 2).all()

    def get_cache_path(self, cal):
        epoch = date_utils.epoch_date_to_string(cal.dateobs.date())
        return os.path.join(self.processed_path, cal.site, cal.camera, epoch, 'processed')

    def download_calibration(self, cal):
        """Download file, validate FITS, write atomically, update DB filepath."""
        dest_dir = self.get_cache_path(cal)
        local_path = os.path.join(dest_dir, cal.filename)

        if os.path.exists(local_path):
            logger.info(f"Already on disk: {cal.filename}, updating DB filepath")
            self._update_filepath(cal.id, dest_dir)
            return
        if cal.frameid is None:
            logger.warning(f"Skipping {cal.filename} - NULL frameid")
            return

        os.makedirs(dest_dir, exist_ok=True)
        logger.info(f"Downloading {cal.filename} (frameid={cal.frameid})")
        buffer = fits_utils.download_from_s3(
            {'frameid': cal.frameid, 'filename': cal.filename},
            self.runtime_context, is_raw_frame=False
        )

        temp_path = local_path + '.tmp'
        try:
            with open(temp_path, 'wb') as f:
                f.write(buffer.read())
            if fits_utils.get_primary_header(temp_path) is None:
                os.remove(temp_path)
                raise ValueError(f"Invalid FITS file: {cal.filename}")
            os.rename(temp_path, local_path)
            self._update_filepath(cal.id, dest_dir)
            logger.info(f"Downloaded {cal.filename}")
        except Exception:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def delete_calibration(self, cal):
        """Remove file from disk and clear filepath in DB."""
        file_path = os.path.join(cal.filepath, cal.filename)
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted {cal.filename}")
        self._update_filepath(cal.id, None)

    def _update_filepath(self, cal_id, filepath):
        with dbs.get_session(self.db_address) as session:
            cal = session.query(dbs.CalibrationImage).get(cal_id)
            if cal:
                cal.filepath = filepath

    def run(self, poll_interval=10):
        """Main loop: poll DB, download missing files, delete stale ones."""
        logger.info("Download worker started")
        last_heartbeat = time.monotonic()

        while True:
            try:
                needed = self.get_calibrations_to_cache()
                needed_filenames = {cal.filename for cal in needed}

                # Download calibrations not yet on local disk
                to_download = [cal for cal in needed
                               if not os.path.exists(os.path.join(self.get_cache_path(cal), cal.filename))]

                # Find locally-cached cals no longer in the top-2 needed set
                with dbs.get_session(self.db_address) as session:
                    cached_in_db = session.query(
                        dbs.CalibrationImage.id, dbs.CalibrationImage.filename,
                        dbs.CalibrationImage.filepath,
                    ).join(dbs.Instrument).filter(
                        dbs.CalibrationImage.filepath.isnot(None),
                        dbs.CalibrationImage.filepath.like(self.processed_path + '%'),
                        dbs.Instrument.site == self.site_id,
                    ).all()
                to_delete = [c for c in cached_in_db if c.filename not in needed_filenames]

                for cal in to_download:
                    try:
                        self.download_calibration(cal)
                    except Exception as e:
                        logger.error(f"Failed to download {cal.filename}: {e}", exc_info=True)
                for cal in to_delete:
                    try:
                        self.delete_calibration(cal)
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
    """Entry point: read env vars, create worker, run."""
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
    runtime_context = Context({
        'ARCHIVE_FRAME_URL': settings.ARCHIVE_FRAME_URL,
        'ARCHIVE_AUTH_HEADER': settings.ARCHIVE_AUTH_HEADER,
        'RAW_DATA_FRAME_URL': settings.RAW_DATA_FRAME_URL,
        'RAW_DATA_AUTH_HEADER': settings.RAW_DATA_AUTH_HEADER,
    })

    worker = DownloadWorker(db_address, site_id, instrument_types, processed_path, runtime_context)
    try:
        worker.run(poll_interval=poll_interval)
    except KeyboardInterrupt:
        logger.info("Download worker stopped")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
