import logging

from banzai import dbs
from banzai.utils import file_utils

logger = logging.getLogger(__name__)


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


def need_to_make_preview(pipeline_context):
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
    image_path = pipeline_context.get_image_path()
    try:
        if not pipeline_context.image_passes_criteria():
            return False

    except dbs.TelescopeMissingException:
        logger.error('Telescope/Camera not in database for {f}'.format(f=image_path))
        return False

    # Get the preview image in db. If it doesn't exist add it.
    preview_image = dbs.get_preview_image(pipeline_context.get_image_path(), db_address=pipeline_context.db_address)
    need_to_process = False
    # Check the md5.
    checksum = file_utils.get_md5(image_path)

    # Reset the number of tries if the file has changed on disk
    if preview_image.checksum != checksum:
        need_to_process = True
        preview_image.checksum = checksum
        preview_image.tries = 0
        preview_image.success = False
        dbs.commit_preview_image(preview_image, pipeline_context.db_address)

    # Check if we need to try again
    elif preview_image.tries < pipeline_context.preview_max_tries and not preview_image.success:
        need_to_process = True
        dbs.commit_preview_image(preview_image, pipeline_context.db_address)

    return need_to_process
