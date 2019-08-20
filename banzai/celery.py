import os
import logging
from datetime import datetime, timedelta
from dateutil.parser import parse

from celery import Celery

from banzai import dbs, calibrations, logs
from banzai.utils import date_utils, realtime_utils, lake_utils, stage_utils
from celery.signals import setup_logging
from banzai.context import Context

app = Celery('banzai')
app.config_from_object('banzai.celeryconfig')
app.conf.update(broker_url=os.getenv('REDIS_HOST', 'redis://localhost:6379/0'))

logger = logging.getLogger('banzai')

RETRY_DELAY = int(os.getenv('RETRY_DELAY', 600))


@setup_logging.connect
def setup_loggers(*args, **kwargs):
    logs.set_log_level(os.getenv('BANZAI_WORKER_LOGLEVEL', 'INFO'))


@app.task(name='celery.schedule_calibration_stacking')
def schedule_calibration_stacking(site: str, runtime_context: dict, min_date=None, max_date=None, frame_types=None):
    runtime_context = Context(runtime_context)
    if min_date is None or max_date is None:
        timezone_for_site = dbs.get_timezone(site, db_address=runtime_context.db_address)
        min_date, max_date = date_utils.get_min_and_max_dates_for_calibration_scheduling(timezone_for_site)

    calibration_blocks = lake_utils.get_calibration_blocks_for_time_range(site, max_date, min_date)

    if frame_types is None:
        frame_types = runtime_context.CALIBRATION_IMAGE_TYPES

    for frame_type in frame_types:
        logger.info('Scheduling stacking', extra_tags={'site': site, 'min_date': min_date, 'max_date': max_date,
                                                       'frame_type': frame_type})

        instruments = dbs.get_instruments_at_site(site=site, db_address=runtime_context.db_address)
        for instrument in instruments:
            logger.info('Checking for scheduled calibration blocks', extra_tags={'site': site, 'min_date': min_date,
                                                                                 'max_date': max_date,
                                                                                 'instrument': instrument.camera,
                                                                                 'frame_type': frame_type})
            blocks_for_calibration = lake_utils.filter_calibration_blocks_for_type(instrument, frame_type,
                                                                                   calibration_blocks)
            if len(blocks_for_calibration) > 0:
                # Set the delay to after the latest block end
                calibration_end_time = max([parse(block['end']) for block in blocks_for_calibration]).replace(tzinfo=None)
                stack_delay = timedelta(seconds=runtime_context.CALIBRATION_STACK_DELAYS[frame_type.upper()])
                now = datetime.utcnow().replace(microsecond=0)
                message_delay = calibration_end_time - now + stack_delay
                if message_delay.days < 0:
                    message_delay_in_seconds = 0  # Remove delay if block end is in the past
                else:
                    message_delay_in_seconds = message_delay.seconds

                schedule_time = now + timedelta(seconds=message_delay_in_seconds)
                logger.info('Scheduling stacking at {}'.format(schedule_time.strftime(date_utils.TIMESTAMP_FORMAT)),
                            extra_tags={'site': site, 'min_date': min_date, 'max_date': max_date,
                                        'instrument': instrument.camera, 'frame_type': frame_type})
                stack_calibrations.apply_async(args=(min_date, max_date, instrument.id, frame_type,
                                                     vars(runtime_context), blocks_for_calibration),
                                               countdown=message_delay_in_seconds)
            else:
                logger.warning('No scheduled calibration blocks found.',
                               extra_tags={'site': site, 'min_date': min_date, 'max_date': max_date,
                                           'instrument': instrument.name, 'frame_type': frame_type})


@app.task(name='celery.stack_calibrations', bind=True, default_retry_delay=RETRY_DELAY)
def stack_calibrations(self, min_date: str, max_date: str, instrument_id: int, frame_type: str,
                       runtime_context: dict, blocks: list):
    runtime_context = Context(runtime_context)
    instrument = dbs.get_instrument_by_id(instrument_id, db_address=runtime_context.db_address)
    logger.info('Checking if we are ready to stack',
                extra_tags={'site': instrument.site, 'min_date': min_date, 'max_date': max_date,
                            'instrument': instrument.camera, 'frame_type': frame_type})

    completed_image_count = len(dbs.get_individual_calibration_images(instrument, frame_type,
                                                                      min_date, max_date, include_bad_frames=True,
                                                                      db_address=runtime_context.db_address))
    expected_image_count = 0
    for block in blocks:
        for molecule in block['molecules']:
            if frame_type.upper() == molecule['type']:
                expected_image_count += molecule['exposure_count']
    if completed_image_count < expected_image_count and self.request.retries < 3:
        logger.info('Number of processed images less than expected. '
                    'Expected: {}, Completed: {}'.format(expected_image_count, completed_image_count),
                    extra_tags={'site': instrument.site, 'min_date': min_date, 'max_date': max_date,
                                'instrument': instrument.camera, 'frame_type': frame_type})
        raise self.retry()
    else:
        logger.info('Starting to stack', extra_tags={'site': instrument.site, 'min_date': min_date,
                                                      'max_date': max_date, 'instrument': instrument.camera,
                                                      'frame_type': frame_type})
        calibrations.make_master_calibrations(instrument, frame_type, min_date, max_date, runtime_context)


@app.task(name='celery.process_image')
def process_image(path: str, runtime_context: dict):
    runtime_context = Context(runtime_context)
    logger.info('Running process image.')
    try:
        if realtime_utils.need_to_process_image(path, runtime_context):
            logger.info('Reducing frame', extra_tags={'filename': os.path.basename(path)})

            # Increment the number of tries for this file
            realtime_utils.increment_try_number(path, db_address=runtime_context.db_address)
            stage_utils.run_pipeline_stages(path, runtime_context)
            realtime_utils.set_file_as_processed(path, db_address=runtime_context.db_address)

    except Exception:
        logger.error("Exception processing frame: {error}".format(error=logs.format_exception()),
                     extra_tags={'filename': os.path.basename(path)})
