import os
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse

from celery import Celery
from kombu import Queue
from celery.exceptions import Retry
from banzai import dbs, calibrations, logs
from banzai.utils import date_utils, realtime_utils, stage_utils
from celery.signals import worker_process_init
from banzai.context import Context
from banzai.utils.observation_utils import filter_calibration_blocks_for_type, get_calibration_blocks_for_time_range
from banzai.utils.date_utils import get_stacking_date_range
import logging


logger = logs.get_logger()

RETRY_DELAY = int(os.getenv('RETRY_DELAY', 600))


# Celery sets up a logger on its own, which messes up the LCOGTFormatter that I want to use. Celery logging in general
# is a nightmare. I read this as a reference:
# https://distributedpython.com/posts/three-ideas-to-customise-celery-logging-handlers/


@worker_process_init.connect
def configure_workers(**kwargs):
    # We need to do this because of how the metrics library uses threads and how celery spawns workers.
    from importlib import reload
    from opentsdb_python_metrics import metric_wrappers
    reload(metric_wrappers)


app = Celery('banzai')
app.config_from_object('banzai.celeryconfig')
app.conf.update(broker_url=os.getenv('TASK_HOST', 'pyamqp://guest@localhost//'),
                worker_hijack_root_logger=False)
celery_task_queue_name = os.getenv('CELERY_TASK_QUEUE_NAME', 'celery')

# Set up custom named celery task queue
# https://docs.celeryproject.org/en/stable/userguide/routing.html#manual-routing
app.conf.task_default_queue = celery_task_queue_name
app.conf.task_queues = (
    Queue(celery_task_queue_name, routing_key=f'{celery_task_queue_name}.#'),
)
app.conf.task_default_exchange = 'tasks'
app.conf.task_default_exchange_type = 'topic'
app.conf.task_default_routing_key = 'task.default'
# Increase broker timeout to avoid re-scheduling tasks that aren't completed within an hour
app.conf.broker_transport_options = {'visibility_timeout': 86400}

app.log.setup()
logs.set_log_level(os.getenv('BANZAI_WORKER_LOGLEVEL', 'INFO'))
logging.getLogger('amqp').setLevel(logging.WARNING)
logging.getLogger('kombu').setLevel(logging.WARNING)
logging.getLogger('celery.bootsteps').setLevel(logging.WARNING)


@app.task(name='celery.schedule_calibration_stacking', reject_on_worker_lost=True, max_retries=5)
def schedule_calibration_stacking(site: str, runtime_context: dict,
                                  min_date: str = None, max_date: str = None, frame_types=None):
    logger.info('Scheduling when to stack frames.', extra_tags={'site': site})
    try:
        runtime_context = Context(runtime_context)

        if min_date is None or max_date is None:
            timezone_for_site = dbs.get_timezone(site, db_address=runtime_context.db_address)
            max_lookback = max(runtime_context.CALIBRATION_LOOKBACK.values())

            block_min_date, block_max_date = date_utils.get_stacking_date_range(timezone_for_site,
                                                                                lookback_days=max_lookback)
        else:
            block_min_date = min_date
            block_max_date = max_date

        calibration_blocks = get_calibration_blocks_for_time_range(site, block_max_date, block_min_date, runtime_context)

        if frame_types is None:
            frame_types = list(runtime_context.CALIBRATION_STACKER_STAGES.keys())

        for frame_type in frame_types:
            if min_date is None or max_date is None:
                lookback = runtime_context.CALIBRATION_LOOKBACK[frame_type]
                stacking_min_date, stacking_max_date = get_stacking_date_range(timezone_for_site,
                                                                               lookback_days=lookback)
            else:
                stacking_min_date = min_date
                stacking_max_date = max_date
            logger.info('Scheduling stacking', extra_tags={'site': site, 'min_date': stacking_min_date,
                                                           'max_date': stacking_max_date, 'frame_type': frame_type})

            instruments = dbs.get_instruments_at_site(site=site, db_address=runtime_context.db_address)
            for instrument in instruments:
                logger.info('Checking for scheduled calibration blocks',
                            extra_tags={'site': site,
                                        'min_date': stacking_min_date,
                                        'max_date': stacking_max_date,
                                        'instrument': instrument.camera,
                                        'frame_type': frame_type})
                blocks_for_calibration = filter_calibration_blocks_for_type(instrument, frame_type,
                                                                            calibration_blocks, runtime_context,
                                                                            stacking_min_date, stacking_max_date)
                if len(blocks_for_calibration) > 0:
                    # Set the delay to after the latest block end
                    calibration_end_time = max([parse(block['end']) for block in blocks_for_calibration])
                    calibration_end_time = calibration_end_time.replace(tzinfo=timezone.utc)
                    stack_delay = timedelta(seconds=runtime_context.CALIBRATION_STACK_DELAYS[frame_type.upper()])
                    now = datetime.now(timezone.utc).replace(microsecond=0)
                    message_delay = calibration_end_time - now + stack_delay
                    if message_delay.days < 0:
                        message_delay_in_seconds = 0  # Remove delay if block end is in the past
                    else:
                        message_delay_in_seconds = message_delay.seconds

                    schedule_time = now + timedelta(seconds=message_delay_in_seconds)
                    logger.info('Scheduling stacking at {}'.format(schedule_time.strftime(date_utils.TIMESTAMP_FORMAT)),
                                extra_tags={'site': site, 'min_date': stacking_min_date, 'max_date': stacking_max_date,
                                            'instrument': instrument.camera, 'frame_type': frame_type})
                    if instrument.nx * instrument.ny > runtime_context.LARGE_WORKER_THRESHOLD:
                        queue_name = runtime_context.LARGE_WORKER_QUEUE
                    else:
                        queue_name = runtime_context.CELERY_TASK_QUEUE_NAME

                    stack_calibrations.apply_async(args=(stacking_min_date, stacking_max_date, instrument.id, frame_type,
                                                         vars(runtime_context), blocks_for_calibration),
                                                   countdown=message_delay_in_seconds, queue=queue_name)
                else:
                    logger.warning('No scheduled calibration blocks found.',
                                   extra_tags={'site': site, 'min_date': min_date, 'max_date': max_date,
                                               'instrument': instrument.name, 'frame_type': frame_type})
    except Exception:
        logger.error("Exception scheduling stacking: {error}".format(error=logs.format_exception()),
                     extra_tags={'site': site})


@app.task(name='celery.stack_calibrations', bind=True, default_retry_delay=RETRY_DELAY, reject_on_worker_lost=True)
def stack_calibrations(self, min_date: str, max_date: str, instrument_id: int, frame_type: str,
                       runtime_context: dict, observations: list):
    try:
        runtime_context = Context(runtime_context)
        instrument = dbs.get_instrument_by_id(instrument_id, db_address=runtime_context.db_address)
        logger.info('Checking if we are ready to stack',
                    extra_tags={'site': instrument.site, 'min_date': min_date, 'max_date': max_date,
                                'instrument': instrument.name, 'frame_type': frame_type})

        completed_image_count = len(dbs.get_individual_cal_frames(instrument, frame_type,
                                                                  min_date, max_date, include_bad_frames=True,
                                                                  db_address=runtime_context.db_address))
        expected_image_count = 0
        for observation in observations:
            for configuration in observation['request']['configurations']:
                if frame_type.upper() == configuration['type']:
                    for instrument_config in configuration['instrument_configs']:
                        expected_image_count += instrument_config['exposure_count']
        logger.info('expected image count: {0}, completed image count: {1}'.format(str(expected_image_count), str(completed_image_count)))
        if completed_image_count < expected_image_count and self.request.retries < 3:
            logger.info('Number of processed images less than expected. '
                        'Expected: {}, Completed: {}'.format(expected_image_count, completed_image_count),
                        extra_tags={'site': instrument.site, 'min_date': min_date, 'max_date': max_date,
                                    'instrument': instrument.camera, 'frame_type': frame_type})
            retry = True
        else:
            logger.info('Starting to stack', extra_tags={'site': instrument.site, 'min_date': min_date,
                                                          'max_date': max_date, 'instrument': instrument.camera,
                                                          'frame_type': frame_type})
            calibrations.make_master_calibrations(instrument, frame_type, min_date, max_date, runtime_context)
            retry = False
    except Exception:
        logger.error("Exception making master frames: {error}".format(error=logs.format_exception()),
                     extra_tags={'frame_type': frame_type, 'instrument_id': instrument_id})
        retry = False

    if retry:
        raise self.retry()


@app.task(name='celery.process_image', bind=True, reject_on_worker_lost=True, max_retries=5)
def process_image(self, file_info: dict, runtime_context: dict):
    """
    :param file_info: Body of queue message: dict
    :param runtime_context: Context object with runtime environment info
    """
    try:
        logger.info('Processing frame', extra_tags={'filename': file_info.get('filename')})
        runtime_context = Context(runtime_context)
        if realtime_utils.need_to_process_image(file_info, runtime_context, self):
            if 'path' in file_info:
                filename = os.path.basename(file_info['path'])
            else:
                filename = file_info.get('filename')
            logger.info('Reducing frame', extra_tags={'filename': filename})
            # Increment the number of tries for this file
            realtime_utils.increment_try_number(filename, db_address=runtime_context.db_address)
            stage_utils.run_pipeline_stages([file_info], runtime_context)
            realtime_utils.set_file_as_processed(filename, db_address=runtime_context.db_address)
    except Retry:
        raise
    except Exception:
        logger.error("Exception processing frame: {error}".format(error=logs.format_exception()),
                     extra_tags={'file_info': file_info})
