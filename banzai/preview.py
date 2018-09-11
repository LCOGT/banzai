import os
import sys
import traceback

from kombu import Exchange, Connection, Queue
from kombu.mixins import ConsumerMixin

from banzai import dbs, trim, bias, dark, flats, qc
from banzai import main
from banzai.main import get_stages_todo
from banzai.utils import file_utils
from banzai import logs
from banzai.utils.image_utils import image_passes_criteria

logger = logs.get_logger(__name__)


def set_preview_file_as_processed(path, db_address=dbs._DEFAULT_DB):
    preview_image = dbs.get_preview_image(path, db_address=db_address)
    if preview_image is not None:
        preview_image.success = True
        dbs.commit_preview_image(preview_image, db_address=db_address)


def increment_preview_try_number(path, db_address=dbs._DEFAULT_DB):
    preview_image = dbs.get_preview_image(path, db_address=db_address)
    # Otherwise increment the number of tries
    preview_image.tries += 1
    dbs.commit_preview_image(preview_image, db_address=db_address)


def need_to_make_preview(path, criteria, db_address=dbs._DEFAULT_DB, max_tries=5):
    """
    Figure out if we need to try to make a preview for a given file.

    Parameters
    ----------
    path: str
          Full path to the image possibly needing a preview reduction
    criteria: iterable
              A list of criterion objects that must be satisfied to produce a preview frame
    db_address: str
                SQLAlchemy style URL to the database with the status of previous preview reductions
    max_tries: int
               Maximum number of retries to make a preview image

    Returns
    -------
    need_preview: bool
                  True if we should try to make a preview reduction

    Notes
    -----
    If the file has changed on disk, we reset the success flags and the number of tries to zero.
    We only attempt to make preview images if the telescope is in the database and is set as
    schedulable.
    """
    try:
        if not image_passes_criteria(path, criteria, db_address=db_address):
            return False

    except dbs.TelescopeMissingException:
        logger.error('Telescope/Camera not in database for {f}'.format(f=path))
        return False

    # Get the preview image in db. If it doesn't exist add it.
    preview_image = dbs.get_preview_image(path, db_address=db_address)
    need_to_process = False
    # Check the md5.
    checksum = file_utils.get_md5(path)

    # Reset the number of tries if the file has changed on disk
    if preview_image.checksum != checksum:
        need_to_process = True
        preview_image.checksum = checksum
        preview_image.tries = 0
        preview_image.success = False
        dbs.commit_preview_image(preview_image, db_address)

    # Check if we need to try again
    elif preview_image.tries < max_tries and not preview_image.success:
        need_to_process = True
        dbs.commit_preview_image(preview_image, db_address)

    return need_to_process


class PreviewModeListener(ConsumerMixin):
    def __init__(self, broker_url, pipeline_context):
        self.broker_url = broker_url
        self.pipeline_context = pipeline_context

    def on_connection_error(self, exc, interval):
        logger.error("{0}. Retrying connection in {1} seconds...".format(exc, interval))
        self.connection = self.connection.clone()
        self.connection.ensure_connection(max_retries=10)

    def get_consumers(self, Consumer, channel):
        consumer = Consumer(queues=[self.queue], callbacks=[self.on_message])
        # Only fetch one thing off the queue at a time
        consumer.qos(prefetch_count=1)
        return [consumer]

    def on_message(self, body, message):
        path = body.get('path')
        message.ack()  # acknowledge to the sender we got this message (it can be popped)

        is_eligible_for_preview = False
        for suffix in preview_eligible_suffixs:
            if suffix in path:
                is_eligible_for_preview = True
                image_suffix = suffix

        if is_eligible_for_preview:
            try:
                if need_to_make_preview(path, self.pipeline_context.allowed_instrument_criteria,
                                        db_address=self.pipeline_context.db_address,
                                        max_tries=self.pipeline_context.max_preview_tries):
                    stages_to_do = get_preview_stages_todo(image_suffix)

                    logging_tags = {'tags': {'filename': os.path.basename(path)}}
                    logger.info('Running preview reduction on {}'.format(path), extra=logging_tags)
                    self.pipeline_context.filename = os.path.basename(path)
                    self.pipeline_context.raw_path = os.path.dirname(path)

                    # Increment the number of tries for this file
                    increment_preview_try_number(path, db_address=self.pipeline_context.db_address)

                    main.run(stages_to_do, self.pipeline_context, image_types=['EXPOSE', 'STANDARD', 'BIAS', 'DARK', 'SKYFLAT'])
                    set_preview_file_as_processed(path, db_address=self.pipeline_context.db_address)

            except Exception as e:
                logging_tags = {'tags': {'filename': os.path.basename(path)}}
                exc_type, exc_value, exc_tb = sys.exc_info()
                tb_msg = traceback.format_exception(exc_type, exc_value, exc_tb)

                logger.error("Exception producing preview frame. {0}. {1}".format(path, tb_msg),
                             extra=logging_tags)


preview_eligible_suffixs = ['e00.fits', 's00.fits', 'b00.fits', 'd00.fits', 'f00.fits']


def run_individual_listener(broker_url, queue_name, pipeline_context):

    fits_exchange = Exchange('fits_files', type='fanout')
    listener = PreviewModeListener(broker_url, pipeline_context)

    with Connection(listener.broker_url) as connection:
        listener.connection = connection.clone()
        listener.queue = Queue(queue_name, fits_exchange)
        try:
            listener.run()
        except listener.connection.connection_errors:
            listener.connection = connection.clone()
            listener.ensure_connection(max_retries=10)
        except KeyboardInterrupt:
            logger.info('Shutting down preview pipeline listener.')


def get_preview_stages_todo(image_suffix):
    if image_suffix == 'b00.fits':
        stages = get_stages_todo(last_stage=trim.Trimmer,
                                 extra_stages=[bias.BiasMasterLevelSubtractor, bias.BiasComparer])
    elif image_suffix == 'd00.fits':
        stages = get_stages_todo(last_stage=bias.BiasSubtractor,
                                 extra_stages=[dark.DarkNormalizer, dark.DarkComparer])
    elif image_suffix == 'f00.fits':
        stages = get_stages_todo(last_stage=dark.DarkSubtractor,
                                 extra_stages=[flats.FlatNormalizer, qc.PatternNoiseDetector, flats.FlatComparer])
    else:
        stages = get_stages_todo()
    return stages
