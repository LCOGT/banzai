import hashlib
import logging
from time import sleep

from lco_ingester import ingester
from lco_ingester.exceptions import RetryError, DoNotRetryError, BackoffRetryError, NonFatalDoNotRetryError

from kombu import Connection, Exchange
from banzai.utils import import_utils

logger = logging.getLogger('banzai')


def post_to_archive_queue(image_path, broker_url, exchange_name='fits_files'):
    exchange = Exchange(exchange_name, type='fanout')
    with Connection(broker_url) as conn:
        producer = conn.Producer(exchange=exchange)
        producer.publish({'path': image_path})
        producer.release()


def post_to_ingester(file_object, image, output_filename):
    logger.info(f'Posting file to the archive', image=image)
    retry = True
    try_counter = 1
    ingester_response = {}
    while retry:
        try:
            ingester_response = ingester.upload_file_and_ingest_to_archive(file_object, path=output_filename)
            logger.debug(f"Ingester response: {ingester_response}", image=image)
            retry = False
        except DoNotRetryError as exc:
            logger.warning('Exception occured: {0}. Aborting.'.format(exc), image=image)
            retry = False
        except NonFatalDoNotRetryError as exc:
            logger.debug('Non-fatal Exception occured: {0}. Aborting.'.format(exc), image=image)
            retry = False
        except RetryError as exc:
            logger.debug('Retry Exception occured: {0}. Retrying.'.format(exc), image=image)
            retry = True
            try_counter += 1
        except BackoffRetryError as exc:
            logger.debug('BackoffRetry Exception occured: {0}. Retrying.'.format(exc), image=image)
            if try_counter > 5:
                logger.warning('Giving up because we tried too many times.', image=image)
                retry = False
            else:
                sleep(5 ** try_counter)
                retry = True
                try_counter += 1
        except Exception as exc:
            logger.fatal('Unexpected exception: {0} Will retry.'.format(exc), image=image)
            retry = True
            try_counter += 1
    return ingester_response


def get_md5(filepath):
    with open(filepath, 'rb') as file:
        md5 = hashlib.md5(file.read()).hexdigest()
    return md5


def ccdsum_to_filename(image):
    if image.binning is None:
        ccdsum_str = ''
    else:
        ccdsum_str = 'bin{ccdsum}'.format(ccdsum=str(image.binning[0]) + 'x' + str(image.binning[1]))
    return ccdsum_str


def filter_to_filename(image):
    return str(image.filter)


def config_to_filename(image):
    filename = str(image.configuration_mode)
    filename = filename.replace('full_frame', '')
    filename = filename.replace('default', '')
    filename = filename.replace('central_2k_2x2', 'center')
    return filename


def telescope_to_filename(image):
    return image.meta.get('TELESCOP', '').replace('-', '')


def make_calibration_filename_function(calibration_type, context):
    def get_calibration_filename(image):
        telescope_filename_function = import_utils.import_attribute(context.TELESCOPE_FILENAME_FUNCTION)
        name_components = {'site': image.instrument.site, 'telescop': telescope_filename_function(image),
                           'camera': image.instrument.camera, 'epoch': image.epoch,
                           'cal_type': calibration_type.lower()}
        cal_file = '{site}{telescop}-{camera}-{epoch}-{cal_type}'.format(**name_components)
        for function_name in context.CALIBRATION_FILENAME_FUNCTIONS[calibration_type]:
            filename_function = import_utils.import_attribute(function_name)
            filename_part = filename_function(image)
            if len(filename_part) > 0:
                cal_file += '-{}'.format(filename_part)
        cal_file += '.fits'
        return cal_file

    return get_calibration_filename
