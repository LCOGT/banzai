import logging

from kombu.mixins import ConsumerMixin

from banzai import dbs
from banzai.utils import file_utils, image_utils, fits_utils
from banzai.celery import process_image

logger = logging.getLogger(__name__)


class RealtimeModeListener(ConsumerMixin):
    def __init__(self, runtime_context):
        self.runtime_context = runtime_context
        self.broker_url = runtime_context.broker_url

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
        process_image.apply_async(args=(path, self.runtime_context._asdict()))
        message.ack()  # acknowledge to the sender we got this message (it can be popped)


def set_file_as_processed(path, db_address=dbs._DEFAULT_DB):
    image = dbs.get_processed_image(path, db_address=db_address)
    if image is not None:
        image.success = True
        dbs.commit_processed_image(image, db_address=db_address)


def increment_try_number(path, db_address=dbs._DEFAULT_DB):
    image = dbs.get_processed_image(path, db_address=db_address)
    # Otherwise increment the number of tries
    image.tries += 1
    dbs.commit_processed_image(image, db_address=db_address)


def need_to_process_image(path, ignore_schedulability=False, db_address=dbs._DEFAULT_DB, max_tries=5):
    """
    Figure out if we need to try to make a process a given file.

    Parameters
    ----------
    path: str
          Full path to the image possibly needing to be processed
    ignore_schedulability: bool
             Process non-schedulable instruments
    db_address: str
                SQLAlchemy style URL to the database with the status of previous reductions
    max_tries: int
               Maximum number of retries to reduce an image

    Returns
    -------
    need_to_process: bool
                  True if we should try to process the image

    Notes
    -----
    If the file has changed on disk, we reset the success flags and the number of tries to zero.
    We only attempt to make images if the instrument is in the database and passes the given criteria.
    """
    logger.info("Checking if file needs to be processed", extra_tags={"filename": path})

    if not (path.endswith('.fits') or path.endswith('.fits.fz')):
        logger.warning("Filename does not have a .fits extension, stopping reduction", extra_tags={"filename": path})
        return False

    header = fits_utils.get_primary_header(path)
    if not image_utils.image_can_be_processed(header, db_address):
        return False

    try:
        instrument = dbs.get_instrument(header, db_address=db_address)
    except ValueError:
        return False
    if not ignore_schedulability and not instrument.schedulable:
        return False

    # Get the image in db. If it doesn't exist add it.
    image = dbs.get_processed_image(path, db_address=db_address)
    need_to_process = False
    # Check the md5.
    checksum = file_utils.get_md5(path)

    # Reset the number of tries if the file has changed on disk
    if image.checksum != checksum:
        need_to_process = True
        image.checksum = checksum
        image.tries = 0
        image.success = False
        dbs.commit_processed_image(image, db_address)

    # Check if we need to try again
    elif image.tries < max_tries and not image.success:
        need_to_process = True
        dbs.commit_processed_image(image, db_address)

    return need_to_process
