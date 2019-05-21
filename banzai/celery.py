import os
import logging
from datetime import datetime, timedelta

from celery import Celery

from banzai import settings, dbs, calibrations, logs
from banzai.utils import date_utils, realtime_utils, lake_utils
from banzai.context import Context
from banzai.utils.stage_utils import run
from celery.signals import setup_logging

app = Celery('banzai')
app.config_from_object('banzai.celeryconfig')
app.conf.update(broker_url=os.getenv('REDIS_HOST', 'redis://localhost:6379/0'))

logger = logging.getLogger('banzai')

RETRY_DELAY = int(os.getenv('RETRY_DELAY', 600))


@setup_logging.connect
def setup_loggers(*args, **kwargs):
    pass


@app.task(name='celery.schedule_calibration_stacking')
def schedule_calibration_stacking(runtime_context_json=None, raw_path=None):
    logger.info('Starting schedule_calibration_stacking for {0}'.format(runtime_context_json['site']))
    timezone_for_site = dbs.get_timezone(runtime_context_json['site'], db_address=runtime_context_json['db_address'])
    min_date, max_date = date_utils.get_min_and_max_dates_for_calibration_scheduling(timezone_for_site,
                                                                                     return_string=True)
    logger.info('scheduling stacking for {0} to {1}'.format(min_date, max_date))
    runtime_context_json['min_date'] = min_date
    runtime_context_json['max_date'] = max_date
    for frame_type in settings.CALIBRATION_IMAGE_TYPES:
        runtime_context_json['frame_type'] = frame_type
        runtime_context = Context(runtime_context_json)
        submit_stacking_tasks_to_queue(runtime_context)


def submit_stacking_tasks_to_queue(runtime_context):
    logger.info('Scheduling stacking checks')
    calibration_blocks = lake_utils.get_calibration_blocks_for_time_range(runtime_context.site,
                                                                          runtime_context.max_date,
                                                                          runtime_context.min_date)
    instruments = dbs.get_instruments_at_site(site=runtime_context.site, db_address=runtime_context.db_address)
    for instrument in instruments:
        logger.info('checking for scheduled calibration blocks for {0} at site {1}'.format(instrument.camera,
                                                                                           instrument.site))
        worker_runtime_context = dict(runtime_context._asdict())
        worker_runtime_context['enclosure'] = instrument.enclosure
        worker_runtime_context['telescope'] = instrument.telescope
        worker_runtime_context['camera'] = instrument.camera
        blocks_for_calibration = lake_utils.filter_calibration_blocks_for_type(instrument,
                                                                               worker_runtime_context['frame_type'],
                                                                               calibration_blocks)
        if len(blocks_for_calibration) > 0:
            # block_end should be the latest block end time
            calibration_end_time = max([datetime.strptime(block['end'], date_utils.TIMESTAMP_FORMAT) for block in blocks_for_calibration])
            stack_delay = timedelta(
                seconds=settings.CALIBRATION_STACK_DELAYS[worker_runtime_context['frame_type'].upper()]
            )
            now = datetime.utcnow().replace(microsecond=0)
            logger.info('before schedule stack for block type {0}'.format(worker_runtime_context['frame_type']))
            message_delay = calibration_end_time - now + stack_delay
            if message_delay.days < 0:
                message_delay_in_seconds = 0  # Remove delay if block end is in the past
            else:
                message_delay_in_seconds = message_delay.seconds
            logger.info('Scheduling stacking for block type {0} with message delay {1}'.format(worker_runtime_context['frame_type'], str(message_delay_in_seconds)))
            stack_calibrations.apply_async(args=(worker_runtime_context, blocks_for_calibration),
                                       countdown=message_delay_in_seconds)


@app.task(name='celery.stack_calibrations', bind=True, default_retry_delay=RETRY_DELAY)
def stack_calibrations(self, runtime_context_json, blocks, process_any_images=True):
    logger.info('schedule stack for matching blocks')
    runtime_context = Context(runtime_context_json)
    min_date = datetime.strptime(runtime_context.min_date, date_utils.TIMESTAMP_FORMAT)
    max_date = datetime.strptime(runtime_context.max_date, date_utils.TIMESTAMP_FORMAT)
    instrument = dbs.query_for_instrument(runtime_context.db_address, runtime_context.site,
                                          runtime_context.camera, enclosure=runtime_context.enclosure,
                                          telescope=runtime_context.telescope)
    completed_image_count = len(dbs.get_individual_calibration_images(instrument, runtime_context.frame_type,
                                                                      min_date, max_date, use_masters=False,
                                                                      db_address=runtime_context.db_address))
    expected_image_count = 0
    for block in blocks:
        for molecule in block['molecules']:
            if runtime_context.frame_type.upper() == molecule['type']:
                expected_image_count += molecule['exposure_count']
    logger.info('expected image count: {0}'.format(str(expected_image_count)))
    logger.info('completed image count: {0}'.format(str(completed_image_count)))
    if completed_image_count < expected_image_count and self.request.retries < 3:
        raise self.retry()
    else:
        calibrations.process_master_maker(runtime_context, instrument, runtime_context.frame_type,
                                          min_date, max_date)


@app.task(name='celery.process_image')
def process_image(path, runtime_context_dict):
    logger.info('Running process image.')
    runtime_context = Context(runtime_context_dict)
    try:
        if realtime_utils.need_to_process_image(path, runtime_context,
                                                db_address=runtime_context.db_address,
                                                max_tries=runtime_context.max_tries):
            logger.info('Reducing frame', extra_tags={'filename': os.path.basename(path)})

            # Increment the number of tries for this file
            realtime_utils.increment_try_number(path, db_address=runtime_context.db_address)

            run(path, runtime_context)
            realtime_utils.set_file_as_processed(path, db_address=runtime_context.db_address)

    except Exception:
        logger.error("Exception processing frame: {error}".format(error=logs.format_exception()),
                     extra_tags={'filename': os.path.basename(path)})
