import os
from glob import glob

import numpy as np
from astropy.io import fits

from banzai import dbs
from banzai import logs
from banzai.utils import file_utils

logger = logs.get_logger(__name__)

def select_images(image_list, image_types):
    images = []
    for filename in image_list:
        try:
            if os.path.splitext(filename)[1] == '.fz':
                ext = 1
            else:
                ext = 0
            if fits.getval(filename, 'OBSTYPE', ext=ext) in image_types:
                images.append(filename)
        except Exception as e:
            logger.error('Unable to get OBSTYPE from {0}.'.format(filename))
            logger.error(e)
            continue

    return images


def make_image_list(pipeline_context):

    search_path = os.path.join(pipeline_context.raw_path)

    if pipeline_context.filename is None:
        # return the list of file and a dummy image configuration
        fits_files = glob(search_path + '/*.fits')
        fz_files = glob(search_path + '/*.fits.fz')

        fz_files_to_remove = []
        for i, f in enumerate(fz_files):
            if f[:-3] in fits_files:
                fz_files_to_remove.append(i)
        fz_files_to_remove.sort(reverse=True)

        for i in fz_files_to_remove:
            fz_files.pop(i)
        image_list = fits_files + fz_files

    else:
        image_list = glob(os.path.join(pipeline_context.raw_path, pipeline_context.filename))
    return image_list


def check_image_homogeneity(images):
    for attribute in ('nx', 'ny', 'ccdsum', 'epoch', 'site', 'instrument'):
        if len({getattr(image, attribute) for image in images}) > 1:
            raise InhomogeneousSetException('Images have different {}s'.format(attribute))
    return images[0]


class InhomogeneousSetException(Exception):
    pass


class MissingCatalogException(Exception):
    pass


def save_images(pipeline_context, images, master_calibration=False):
    for image in images:
        output_directory = file_utils.make_output_directory(pipeline_context, image)
        if not master_calibration:
            image.filename = image.filename.replace('00.fits',
                                                    '{:02d}.fits'.format(int(pipeline_context.rlevel)))

        image_filename = os.path.basename(image.filename)
        filepath = os.path.join(output_directory, image_filename)
        image.writeto(filepath, pipeline_context.fpack)
        if pipeline_context.fpack:
            image_filename += '.fz'
            filepath += '.fz'
        if master_calibration:
            dbs.save_calibration_info(image.obstype, filepath, image,
                                      db_address=pipeline_context.db_address)

        if pipeline_context.post_to_archive:
            logger.info('Posting {filename} to the archive'.format(filename=image_filename))
            try:
                file_utils.post_to_archive_queue(filepath)
            except Exception as e:
                logger.error("Could not post {0} to ingester.".format(filepath))
                logger.error(e)
                continue


def get_bpm(image, pipeline_context):
    bpm_filename = dbs.get_bpm(image.telescope_id, image.ccdsum,
                               db_address=pipeline_context.db_address)
    if bpm_filename is None:
        bpm_data = np.zeros((image.ny, image.nx), dtype=np.uint8)
        image.header['L1IDMASK'] = ('', 'Id. of mask file used')
    else:
        bpm_data = fits.getdata(bpm_filename)
        image.header['L1IDMASK'] = (os.path.basename(bpm_filename), 'Id. of mask file used')

    return np.array(bpm_data, dtype=np.uint8)
