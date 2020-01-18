import datetime
import hashlib
import os
import logging
from time import sleep

import requests

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


def post_to_ingester(filepath):
    retry = True
    try_counter = 1
    ingester_response = {}
    with open(filepath, 'rb') as f:
        while retry:
            try:
                ingester_response = ingester.upload_file_and_ingest_to_archive(f)
                retry = False
            except DoNotRetryError as exc:
                logger.warning('Exception occured: {0}. Aborting.'.format(exc),
                               extra_tags={'filename': filepath})
                retry = False
            except NonFatalDoNotRetryError as exc:
                logger.debug('Non-fatal Exception occured: {0}. Aborting.'.format(exc),
                             extra_tags={'filename': filepath})
                retry = False
            except RetryError as exc:
                logger.debug('Retry Exception occured: {0}. Retrying.'.format(exc),
                             extra_tags={'tags': {'filename': filepath}})
                retry = True
                try_counter += 1
            except BackoffRetryError as exc:
                logger.debug('BackoffRetry Exception occured: {0}. Retrying.'.format(exc),
                             extra_tags={'filename': filepath})
                if try_counter > 5:
                    logger.warning('Giving up because we tried too many times.', extra_tags={'filename': filepath})
                    retry = False
                else:
                    sleep(5 ** try_counter)
                    retry = True
                    try_counter += 1
            except Exception as exc:
                logger.fatal('Unexpected exception: {0} Will retry.'.format(exc), extra_tags={'filename': filepath})
                retry = True
                try_counter += 1

    return ingester_response


def make_output_directory(runtime_context, image_config):
    # Create output directory if necessary
    output_directory = os.path.join(runtime_context.processed_path, image_config.site,
                                    image_config.instrument.name, image_config.epoch)

    if runtime_context.preview_mode:
        output_directory = os.path.join(output_directory, 'preview')
    else:
        output_directory = os.path.join(output_directory, 'processed')

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    return output_directory


def get_md5(filepath):
    with open(filepath, 'rb') as file:
        md5 = hashlib.md5(file.read()).hexdigest()
    return md5


def instantly_public(proposal_id):
    public_now = False
    if proposal_id in ['calibrate', 'standard', 'pointing']:
        public_now = True
    if 'epo' in proposal_id.lower():
        public_now = True
    return public_now


def ccdsum_to_filename(image):
    if image.ccdsum is None:
        ccdsum_str = ''
    else:
        ccdsum_str = 'bin{ccdsum}'.format(ccdsum=image.ccdsum.replace(' ', 'x'))
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
    return image.header.get('TELESCOP', '').replace('-', '')


def make_calibration_filename_function(calibration_type, context):
    def get_calibration_filename(image):
        telescope_filename_function = import_utils.import_attribute(context.TELESCOPE_FILENAME_FUNCTION)
        name_components = {'site': image.site, 'telescop': telescope_filename_function(image),
                           'camera': image.header.get('INSTRUME', ''), 'epoch': image.epoch,
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


def get_basename(path):
    basename = None
    if path is not None:
        filename = os.path.basename(path)
        if filename.find('.') > 0:
            basename = filename[:filename.index('.')]
        else:
            basename = filename
    return basename


def download_from_s3(file_info, output_directory, runtime_context):
    frame_id = file_info.get('frameid')
    if frame_id is not None:
        url = f'{runtime_context.ARCHIVE_FRAME_URL}/{frame_id}'
    else:
        basename = get_basename(file_info.get('path'))
        url = f'{runtime_context.ARCHIVE_FRAME_URL}/?basename={basename}'

    response = requests.get(url, headers=runtime_context.ARCHIVE_AUTH_TOKEN, stream=True).json()
    path = os.path.join(output_directory, response['filename'])
    with open(path, 'wb') as f:
        f.write(requests.get(response['url']).content)
    return path
