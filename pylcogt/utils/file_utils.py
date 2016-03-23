import os
from kombu import Connection, Queue, Exchange
from glob import glob
from astropy.io import fits
import numpy as np

from pylcogt import dbs
from pylcogt import logs
from pylcogt.images import Image


__author__ = 'cmccully'

logger = logs.get_logger(__name__)


def make_output_directory(pipeline_context, image):
    # Get the telescope from the image
    telescope = dbs.get_telescope(image)
    # Create output directory if necessary
    output_directory = os.path.join(pipeline_context.processed_path, telescope.site,
                                    telescope.instrument, image.dayobs)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    return


def post_to_archive_queue(image_path):
    def errback(exc, interval):
        logger.error('Error: %r', exc, exc_info=1)
        logger.info('Retry in %s seconds.', interval)
    fits_exchange = Exchange('fits_files', type='fanout')
    producer_queue = Queue('pipeline', fits_exchange, exclusive=True)
    with Connection('amqp://guest:guest@cerberus.lco.gtn') as conn:
        queue = conn.SimpleQueue(producer_queue)
        put = conn.ensure(queue, queue.put, max_retries=30, errback=errback)
        put({'path': image_path})


def get_bpm(image):
    bpm_filename = dbs.get_bpm(image.telescope_id, image.ccdsum)
    if bpm_filename is None:
        bpm_data = np.zeros((image.ny, image.nx), dtype=np.uint8)
        image.header['L1IDMASK'] = ''
    else:
        bpm_data = fits.getdata(bpm_filename)
        image.header['L1IDMASK'] = os.path.basename(bpm_filename)

    return np.array(bpm_data, dtype=np.uint8)


def read_images(image_list):
    images = []
    for filename in image_list:
        try:
            image = Image(filename=filename)
            if image.bpm is None:
                image.bpm = get_bpm(image)
            images.append(image)
        except Exception as e:
            logger.error('Error loading {0}'.format(filename))
            logger.error(e)
            continue
    return images


def save_images(pipeline_context, images, master_calibration=False):
    for image in images:
        if not master_calibration:
            image.filename = image.filename.replace('00.fits',
                                                    '{:02d}.fits'.format(pipeline_context.rlevel))

        image_filename = os.path.basename(image.filename)
        filepath = os.path.join(pipeline_context.processed_path, image_filename)
        image.writeto(filepath, pipeline_context.fpack)
        if pipeline_context.fpack:
            image_filename += '.fz'
            filepath += '.fz'
        if master_calibration:
            dbs.save_calibration_info(image.obstype, filepath, image)

        if pipeline_context.post_to_archive:
            logger.info('Posting {filename} to the archive'.format(filename=image_filename))
            try:
                post_to_archive_queue(filepath)
            except Exception as e:
                logger.error("Could not post {0} to ingester.".format(filepath))
                logger.error(e)
                continue


def make_image_list(pipeline_context):

    search_path = os.path.join(pipeline_context.raw_path)

    # return the list of file and a dummy image configuration
    return glob(search_path + '/*.fits')


def select_images(image_list, image_type):
    images = []
    for filename in image_list:
        try:
            if fits.getval(filename, 'OBSTYPE') == image_type:
                images.append(filename)
        except Exception as e:
            logger.error('Unable to get OBSTYPE from {0}.'.format(filename))
            logger.error(e)
            continue

    return images
