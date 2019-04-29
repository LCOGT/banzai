import os
from glob import glob
import logging

from banzai import settings, logs
from banzai import logs
from banzai import dbs
from banzai.images import logger
from banzai.munge import munge
from banzai.utils.fits_utils import get_primary_header
from banzai.utils.instrument_utils import instrument_passes_criteria
from banzai.utils import import_utils
from banzai.exceptions import InhomogeneousSetException


logger = logging.getLogger('banzai')


FRAME_CLASS = import_utils.import_attribute(settings.FRAME_CLASS)


def get_obstype(header):
    return header.get('OBSTYPE', None)


def get_reduction_level(header):
    return header.get('RLEVEL', '00')


def select_images(image_list, image_type, db_address, ignore_schedulability):
    images = []
    for filename in image_list:
        try:
            header = get_primary_header(filename)
            should_process = image_can_be_processed(header, db_address)
            should_process &= (image_type is None or get_obstype(header) == image_type)
            if not ignore_schedulability:
                instrument = dbs.get_instrument(header, db_address=db_address)
                should_process &= instrument.schedulable
            if should_process:
                images.append(filename)
        except Exception:
            logger.error(logs.format_exception(), extra_tags={'filename': filename})
    return images


def make_image_path_list(raw_path):
    if os.path.isdir(raw_path):
        # return the list of file and a dummy image configuration
        fits_files = glob(os.path.join(raw_path, '*.fits'))
        fz_files = glob(os.path.join(raw_path, '*.fits.fz'))

        fz_files_to_remove = []
        for i, f in enumerate(fz_files):
            if f[:-3] in fits_files:
                fz_files_to_remove.append(f)

        for f in fz_files_to_remove:
            fz_files.remove(f)
        image_path_list = fits_files + fz_files

    else:
        image_path_list = glob(raw_path)
    return image_path_list


def check_image_homogeneity(images, group_by_attributes=None):
    attribute_list = ['nx', 'ny', 'site', 'camera']
    if group_by_attributes is not None:
        attribute_list += group_by_attributes
    for attribute in attribute_list:
        if len(set([getattr(image, attribute) for image in images])) > 1:
            raise InhomogeneousSetException('Images have different {0}s'.format(attribute))


def image_can_be_processed(header, db_address):
    if header is None:
        logger.warning('Header being checked to process image is None')
        return False
    # Short circuit if the instrument is a guider even if they don't exist in configdb
    if not get_obstype(header) in settings.LAST_STAGE:
        logger.warning('Image has an obstype that is not supported by banzai.')
        return False
    try:
        instrument = dbs.get_instrument(header, db_address=db_address)
    except ValueError:
        return False
    passes = instrument_passes_criteria(instrument, settings.FRAME_SELECTION_CRITERIA)
    if not passes:
        logger.debug('Image does not pass reduction criteria')
    passes &= get_reduction_level(header) == '00'
    if get_reduction_level(header) != '00':
        logger.debug('Image has nonzero reduction level')
    return passes


def read_image(filename, runtime_context):
    try:
        image = FRAME_CLASS(runtime_context, filename=filename)
        if image.instrument is None:
            logger.error("Image instrument attribute is None, aborting", image=image)
            raise IOError
        munge(image)
        return image
    except Exception:
        logger.error('Error loading image: {error}'.format(error=logs.format_exception()),
                     extra_tags={'filename': filename})
