import hashlib
import os
import logging

from kombu import Connection, Exchange

logger = logging.getLogger('banzai')


def post_to_archive_queue(image_path, broker_url, exchange_name='fits_files'):
    exchange = Exchange(exchange_name, type='fanout')
    with Connection(broker_url) as conn:
        producer = conn.Producer(exchange=exchange)
        producer.publish({'path': image_path})
        producer.release()


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
