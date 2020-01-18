import logging

from banzai.utils.fits_utils import get_primary_header

from banzai import dbs
from banzai.utils import image_utils, file_utils, fits_utils

logger = logging.getLogger('banzai')


def set_file_as_processed(filename, db_address=dbs._DEFAULT_DB):
    image = dbs.get_processed_image(filename, db_address=db_address)
    if image is not None:
        image.success = True
        dbs.commit_processed_image(image, db_address=db_address)


def increment_try_number(filename, db_address=dbs._DEFAULT_DB):
    image = dbs.get_processed_image(filename, db_address=db_address)
    # Otherwise increment the number of tries
    image.tries += 1
    dbs.commit_processed_image(image, db_address=db_address)


def need_to_process_image(file_info, context):
    """
    Figure out if we need to try to make a process a given file.

    Parameters
    ----------
    file_info: dict
          Message body from LCO fits queue
    context: banzai.context.Context
          Context object with runtime environment info

    Returns
    -------
    need_to_process: bool
          True if we should try to process the image

    Notes
    -----
    If the file has changed, we reset the success flags and the number of tries to zero.
    We only attempt to make images if the instrument is in the database and passes the given criteria.
    """
    filename = fits_utils.get_filename_from_info(file_info)
    is_s3_message = fits_utils.is_s3_queue_message(file_info)

    logger.info("Checking if file needs to be processed", extra_tags={"filename": filename})
    if not (filename.endswith('.fits') or filename.endswith('.fits.fz')):
        logger.warning("Filename does not have a .fits extension, stopping reduction",
                       extra_tags={"filename": filename})
        return False

    if is_s3_message:
        header = file_info
        checksum = header['version_set'][0]['md5']
    else:
        path = fits_utils.get_local_path_from_info(file_info, context)
        header = get_primary_header(file_info, context)
        checksum = file_utils.get_md5(path)

    if not image_utils.image_can_be_processed(header, context):
        return False

    try:
        instrument = dbs.get_instrument(header, db_address=context.db_address)
    except ValueError:
        return False
    if not context.ignore_schedulability and not instrument.schedulable:
        logger.info('Image will not be processed because instrument is not schedulable',
                    extra_tags={"filename": filename})
        return False

    # Get the image in db. If it doesn't exist add it.
    image = dbs.get_processed_image(filename, db_address=context.db_address)
    need_to_process = False

    # If this is an message on the archived_fits queue, then update the frameid
    if is_s3_message:
        image.frameid = file_info.get('frameid')

    # Check the md5.
    # Reset the number of tries if the file has changed on disk/in s3
    if image.checksum != checksum:
        need_to_process = True
        image.checksum = checksum
        image.tries = 0
        image.success = False
        dbs.commit_processed_image(image, context.db_address)

    # Check if we need to try again
    elif image.tries < context.max_tries and not image.success:
        need_to_process = True
        dbs.commit_processed_image(image, context.db_address)

    return need_to_process
