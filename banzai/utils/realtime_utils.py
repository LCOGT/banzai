import logging
import os

from banzai import dbs
from banzai.utils import fits_utils, image_utils, file_utils, realtime_utils

logger = logging.getLogger('banzai')


def get_filename_from_info(file_info):
    """
    Get a filename from a queue message
    :param file_info: Queue message body: dict
    :return: filename : str

    When running using a /archive mount, BANZAI listens on the fits_queue, which contains a
    path to an image on the archive machine. When running using AWS and s3, we listen to archived_fits
    which contains a complete dictionary of image parameters, one of which is a filename including extension.
    """
    path = file_info.get('path')
    if path is None:
        path = file_info.get('filename')
    return os.path.basename(path)


def get_local_path_from_info(file_info, runtime_context):
    """
    Given a message from an LCO fits queue, determine where the image would
    be stored locally by the pipeline.
    :param file_info: Queue message body: dict
    :param runtime_context: Context object with runtime environment info
    :return: filepath: str
    """
    if is_s3_queue_message(file_info):
        # archived_fits contains a dictionary of image attributes and header values
        path = os.path.join(runtime_context.processed_path, file_info.get('SITEID'),
                            file_info.get('INSTRUME'), file_info.get('DAY-OBS'))

        if file_info.get('RLEVEL') == 0:
            path = os.path.join(path, 'raw')
        elif file_info.get('RLEVEL') == 91:
            path = os.path.join(path, 'processed')

        path = os.path.join(path, file_info.get('filename'))
    else:
        # fits_queue contains paths to images on /archive
        path = file_info.get('path')

    return path


def need_to_get_from_s3(file_info, runtime_context):
    """
    Determine whether we need to retrieve a file from s3
    If it does not exist locally, and the queue message indicates we are operating
    using s3, then we must pull the image down
    :param file_info: Queue message body: dict
    :param runtime_context: Context object with runtime environment info
    :return: True if we should read from s3, else False
    """
    local_file_path = get_local_path_from_info(file_info, runtime_context)
    return is_s3_queue_message(file_info) and not os.path.isfile(local_file_path)


def is_s3_queue_message(file_info):
    """
    Determine if we are reading from s3 based on the contents of the
    message on the queue
    :param file_info: Queue message body: dict
    :return: True if we should read from s3, else False
    """
    s3_queue_message = False
    path_to_file = file_info.get('path')
    if path_to_file is None:
        s3_queue_message = True

    return s3_queue_message


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
    filename = realtime_utils.get_filename_from_info(file_info)
    download_from_s3 = need_to_get_from_s3(file_info)

    logger.info("Checking if file needs to be processed", extra_tags={"filename": filename})
    if not (filename.endswith('.fits') or filename.endswith('.fits.fz')):
        logger.warning("Filename does not have a .fits extension, stopping reduction", extra_tags={"filename": path})
        return False

    if download_from_s3:
        header = file_info
        checksum = header['version_set']['md5']
    else:
        path = get_local_path_from_info(file_info, context)
        header = fits_utils.get_primary_header(path)
        checksum = file_utils.get_md5(path)

    if not image_utils.image_can_be_processed(header, context):
        return False

    try:
        instrument = dbs.get_instrument(header, db_address=context.db_address)
    except ValueError:
        return False
    if not context.ignore_schedulability and not instrument.schedulable:
        logger.info('Image will not be processed because instrument is not schedulable', extra_tags={"filename": filename})
        return False

    # Get the image in db. If it doesn't exist add it.
    image = dbs.get_processed_image(filename, db_address=context.db_address)
    need_to_process = False

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
