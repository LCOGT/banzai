import os
import logging

from celery import Celery

from celery.schedules import crontab
from banzai import settings, dbs, calibrations, logs
from banzai.utils import date_utils, realtime_utils
from banzai.context import Context
from banzai.utils.stage_utils import run

app = Celery('banzai')
app.config_from_object('banzai.celeryconfig')
app.conf.update(broker_url=os.getenv('REDIS_HOST', 'redis://localhost:6379/0'))

logger = logging.getLogger(__name__)

RETRY_DELAY = int(os.getenv('RETRY_DELAY', 600))


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
        calibrations.schedule_stacking_checks(runtime_context)


@app.task(name='celery.schedule_stack', bind=True, default_retry_delay=RETRY_DELAY)
def schedule_stack(self, runtime_context_json, blocks, process_any_images=True):
    logger.info('schedule stack for matching blocks')
    runtime_context = Context(runtime_context_json)
    instrument = dbs.query_for_instrument(runtime_context.db_address, runtime_context.site,
                                          runtime_context.camera, enclosure=runtime_context.enclosure,
                                          telescope=runtime_context.telescope)
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


@app.on_after_configure.connect
def setup_stacking_schedule(sender, runtime_context, raw_path, **kwargs):
    for site, entry in settings.SCHEDULE_STACKING_CRON_ENTRIES.items():
        runtime_context_json = dict(runtime_context._asdict())
        runtime_context_json['site'] = site
        worker_runtime_context = Context(runtime_context_json)
        sender.add_periodic_task(
            crontab(minute=entry['minute'], hour=entry['hour']),
            schedule_calibration_stacking.s(runtime_context=worker_runtime_context, raw_path=raw_path)
        )


@app.task(name='celery.process_image')
def process_image(path, runtime_context_dict):
    logger.info('Got into actor.')
    runtime_context = Context(runtime_context_dict)
    try:
        # pipeline_context = PipelineContext.from_dict(pipeline_context_json)
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
