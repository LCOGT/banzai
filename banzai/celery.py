import os
import logging

from datetime import datetime, timedelta
from celery import Celery

from banzai import settings, dbs, calibrations
from banzai.utils import lake_utils, date_utils
from banzai.context import Context

app = Celery('banzai')
app.conf.broker_url = os.getenv('REDIS_HOST', '127.0.0.1')

logger = logging.getLogger(__name__)

RETRY_DELAY = int(os.getenv('RETRY_DELAY', 600))


# @dramatiq.actor(queue_name=settings.REDIS_QUEUE_NAMES['SCHEDULE_STACK'])
@app.task(name='celery.schedule_calibration_stacking')
def schedule_calibration_stacking(runtime_context=None, raw_path=None):
    logger.info('starting schedule_calibration_stacking for end-to-end tests')
    timezone_for_site = dbs.get_timezone(runtime_context.site, db_address=runtime_context.db_address)
    min_date, max_date = date_utils.get_min_and_max_dates_for_calibration_scheduling(timezone_for_site, return_string=True)
    logger.info('scheduling stacking for {0} to {1}'.format(min_date, max_date))
    runtime_context_json = dict(runtime_context._asdict())
    runtime_context_json['min_date'] = min_date
    runtime_context_json['max_date'] = max_date
    for frame_type in settings.CALIBRATION_IMAGE_TYPES:
        runtime_context_json['frame_type'] = frame_type
        runtime_context = Context(runtime_context_json)
        schedule_stacking_checks(runtime_context)


# @dramatiq.actor(max_retries=0, queue_name=settings.REDIS_QUEUE_NAMES['SCHEDULE_STACK'])
# @app.task(name='celery.should_retry_schedule_stack')
# def should_retry_schedule_stack(message_data, exception_data):
#     logger.info('Failed to process message, retrying')
#     if message_data['options']['retries'] >= 2:
#         schedule_stack(*message_data['args'], process_any_images=True)


# @dramatiq.actor(max_retries=3, min_backoff=RETRY_DELAY, max_backoff=RETRY_DELAY, queue_name=settings.REDIS_QUEUE_NAMES['SCHEDULE_STACK'])
@app.task(name='celery.schedule_stack', bind=True, default_retry_delay=RETRY_DELAY)
def schedule_stack(self, runtime_context_json, blocks, process_any_images=True):
    logger.info('schedule stack for matching blocks')
    runtime_context = Context(runtime_context_json)
    instrument = dbs.query_for_instrument(runtime_context.db_address, runtime_context.site,
                                          runtime_context.camera, runtime_context.enclosure,
                                          runtime_context.telescope)
    completed_image_count = len(dbs.get_individual_calibration_images(instrument, runtime_context.frame_type,
                                                                      runtime_context.min_date,
                                                                      runtime_context.max_date,
                                                                      use_masters=False,
                                                                      db_address=runtime_context.db_address))
    expected_image_count = 0
    for block in blocks:
        for molecule in block['molecules']:
            if runtime_context.frame_type.upper() == molecule['type']:
                expected_image_count += molecule['exposure_count']
    logger.info('expected image count: {0}'.format(str(expected_image_count)))
    logger.info('completed image count: {0}'.format(str(completed_image_count)))
    if (completed_image_count < expected_image_count and self.request.retries < 3):
        raise self.retry()
    else:
        calibrations.process_master_maker(runtime_context, instrument, runtime_context.frame_type,
                                          runtime_context.min_date, runtime_context.max_date)


def schedule_stacking_checks(runtime_context):
    logger.info('scheduling stacking checks')
    calibration_blocks = lake_utils.get_calibration_blocks_for_time_range(runtime_context.site,
                                                                          runtime_context.max_date,
                                                                          runtime_context.min_date)
    instruments = dbs.get_instruments_at_site(site=runtime_context.site, db_address=runtime_context.db_address)
    for instrument in instruments:
        worker_runtime_context = dict(runtime_context._asdict())
        worker_runtime_context['enclosure'] = instrument.enclosure
        worker_runtime_context['telescope'] = instrument.telescope
        worker_runtime_context['camera'] = instrument.camera
        blocks_for_calibration = lake_utils.filter_calibration_blocks_for_type(instrument,
                                                                               worker_runtime_context['frame_type'],
                                                                               calibration_blocks)
        if len(blocks_for_calibration) > 0:
            block_end = datetime.strptime(blocks_for_calibration[0]['end'], date_utils.TIMESTAMP_FORMAT)
            stack_delay = timedelta(
                seconds=settings.CALIBRATION_STACK_DELAYS[worker_runtime_context['frame_type'].upper()]
            )
            now = datetime.utcnow().replace(microsecond=0)
            logger.info('before schedule stack for block type {0}'.format(worker_runtime_context['frame_type']))
            message_delay = block_end - now + stack_delay
            if message_delay.days < 0:
                message_delay_in_seconds = 0  # Remove delay if block end is in the past
            else:
                message_delay_in_seconds = message_delay.seconds
            logger.info('scheduling with message delay {0}'.format(str(message_delay_in_seconds)))
            schedule_stack.apply_async(args=(worker_runtime_context, blocks_for_calibration),
                                       eta=message_delay_in_seconds)
