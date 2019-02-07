import logging

from banzai import dbs
from banzai.utils import file_utils
from banzai.utils.image_utils import image_passes_criteria

logger = logging.getLogger(__name__)


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


def need_to_process_image(path, context, db_address=dbs._DEFAULT_DB, max_tries=5):
    """
    Figure out if we need to try to make a process a given file.

    Parameters
    ----------
    path: str
          Full path to the image possibly needing to be processed
    context: iterable
             A pipeline context with FRAME_SELECTION_CRITERIA
    db_address: str
                SQLAlchemy style URL to the database with the status of previous reductions
    max_tries: int
               Maximum number of retries to reduce an image

    Returns
    -------
    need_preview: bool
                  True if we should try to process the image

    Notes
    -----
    If the file has changed on disk, we reset the success flags and the number of tries to zero.
    We only attempt to make images if the instrument is in the database and passes the given criteria.
    """
    logger.info("Checking if file needs to be processed", extra_tags={"filename": path})

    if not image_passes_criteria(path, context, db_address=db_address):
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
