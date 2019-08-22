import hashlib
import os
import logging

from kombu import Connection, Exchange
import datetime
from banzai.utils import import_utils, date_utils
import banzai

logger = logging.getLogger('banzai')


def post_to_archive_queue(image_path, broker_url, exchange_name='fits_files'):
    exchange = Exchange(exchange_name, type='fanout')
    with Connection(broker_url) as conn:
        producer = conn.Producer(exchange=exchange)
        producer.publish({'path': image_path})
        producer.release()


def make_output_directory(top_path, site, instrument_name, epoch, preview_mode=False):
    # Create output directory if necessary
    output_directory = os.path.join(top_path.processed_path, site,
                                    instrument_name, epoch)

    if preview_mode:
        output_directory = os.path.join(output_directory, 'preview')
    else:
        output_directory = os.path.join(output_directory, 'processed')

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    return output_directory


def make_output_filename(filename: str, fpack: bool, reduction_level: str):
    filename = os.path.basename(filename.replace('00.fits', '{:02d}.fits'.format(int(reduction_level))))
    if fpack and not filename.endswith('.fz'):
        filename += '.fz'
    return filename


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


def save_pipeline_metadata(header, reduction_level):
    datecreated = datetime.datetime.utcnow()
    header['DATE'] = (date_utils.date_obs_to_string(datecreated), '[UTC] Date this FITS file was written')
    header['RLEVEL'] = (reduction_level, 'Reduction level')

    header['PIPEVER'] = (banzai.__version__, 'Pipeline version')

    if instantly_public(header['PROPID']):
        header['L1PUBDAT'] = (header['DATE-OBS'], '[UTC] Date the frame becomes public')
    else:
        # Wait a year
        date_observed = date_utils.parse_date_obs(header['DATE-OBS'])
        next_year = date_observed + datetime.timedelta(days=365)
        header['L1PUBDAT'] = (date_utils.date_obs_to_string(next_year), '[UTC] Date the frame becomes public')
