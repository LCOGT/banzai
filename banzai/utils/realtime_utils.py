import os

from banzai import dbs
from banzai.utils import file_utils, import_utils, image_utils
from banzai.data import HeaderOnly
from banzai import logs
import datetime


logger = logs.get_logger()


def set_file_as_processed(path, db_address):
    image = dbs.get_processed_image(path, db_address=db_address)
    if image is not None:
        image.success = True
        dbs.commit_processed_image(image, db_address=db_address)


def increment_try_number(path, db_address):
    image = dbs.get_processed_image(path, db_address=db_address)
    # Otherwise increment the number of tries
    image.tries += 1
    dbs.commit_processed_image(image, db_address=db_address)


def need_to_process_image(file_info, context, task):
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
    if 'path' not in file_info and 'frameid' not in file_info:
        logger.error('Ill formed queue message. Aborting')
        return False

    if 'frameid' in file_info:
        if 'version_set' not in file_info:
            logger.info("Version set not available in file_info", extra_tags={"filename": file_info['filename']})
            return True
        checksum = file_info['version_set'][0].get('md5')
        filename = file_info['filename']
    else:
        filename = os.path.basename(file_info['path'])
        checksum = file_utils.get_md5(file_info['path'])

    logger.info("Checking if file needs to be processed", extra_tags={"filename": filename})
    if not (filename.endswith('.fits') or filename.endswith('.fits.fz')):
        logger.error("Filename does not have a .fits extension, stopping reduction",
                     extra_tags={"filename": filename})
        return False

    # Get the image in db. If it doesn't exist add it.
    image = dbs.get_processed_image(filename, db_address=context.db_address)
    # If this is an message on the archived_fits queue, then update the frameid
    image.frameid = file_info.get('frameid')

    need_to_process = False
    # Check the md5.
    # Reset the number of tries if the file has changed on disk/in s3
    if image.checksum != checksum:
        logger.info('File has changed on disk. Resetting success flags and tries', extra_tags={'filename': filename})
        need_to_process = True
        image.checksum = checksum
        image.tries = 0
        image.success = False
        dbs.commit_processed_image(image, context.db_address)

    # Check if we need to try again
    elif image.tries < context.max_tries and not image.success:
        logger.info('File has not been successfully processed yet. Trying again.', extra_tags={'filename': filename})
        need_to_process = True
        dbs.commit_processed_image(image, context.db_address)

    # if we are pulling off the archived fits queue, make sure that the header can make a valid image object before
    # bothering to pull it from s3
    if 'frameid' in file_info:
        try:
            factory = import_utils.import_attribute(context.FRAME_FACTORY)()
            test_image = factory.observation_frame_class(hdu_list=[HeaderOnly(file_info, name='')],
                                                         file_path=file_info['filename'])
            try:
                test_image.instrument = factory.get_instrument_from_header(file_info, db_address=context.db_address)
            except Exception:
                logger.error(f'Issue getting instrument from header. {logs.format_exception()}', extra_tags={'filename': filename})
                need_to_process = False
            if image_utils.get_reduction_level(test_image.meta) != '00':
                logger.error('Image has nonzero reduction level. Aborting.', extra_tags={'filename': filename})
                need_to_process = False
            elif test_image.instrument is None:
                logger.error('This queue message has an instrument that is not currently in the DB. Aborting:',
                             extra_tags={'filename': filename})
                need_to_process = False
            elif not image_utils.image_can_be_processed(test_image, context):
                msg = 'The header in this queue message appears to not be complete enough to make a Frame object'
                logger.error(msg, extra_tags={'filename': filename})
                need_to_process = False
            if context.delay_to_block_end and test_image.obstype in context.OBSTYPES_TO_DELAY:
                if datetime.datetime.now() < test_image.block_end_date:
                    logger.info('Observing Block in progress. Retrying 5 minutes after it completes',
                                extra_tags={'filename': filename})
                    delay = test_image.block_end_date - datetime.datetime.now() + datetime.timedelta(minutes=5)
                    task.retry(countdown=delay.total_seconds())
        except Exception:
            logger.error('Issue creating Image object with given queue message', extra_tags={"filename": filename})
            logger.error(logs.format_exception())
            need_to_process = False

    return need_to_process
