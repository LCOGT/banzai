import os
from glob import glob
import logging

import banzai.context
from banzai import logs
from banzai import dbs
from banzai.utils.fits_utils import get_primary_header

logger = logging.getLogger(__name__)


def image_can_be_processed(header, context):
    instrument = dbs.get_instrument(header, db_address=context.db_address)
    passes = banzai.context.instrument_passes_criteria(instrument, context.FRAME_SELECTION_CRITERIA)
    passes &= context.can_process(header)
    return passes


def get_obstype(header):
    return header.get('OBSTYPE', None)


def select_images(image_list, context, image_type):
    images = []
    for filename in image_list:
        try:
            header = get_primary_header(filename)
            if image_can_be_processed(header, context) and (image_type is None or get_obstype(header) == image_type):
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


def get_calibration_image_path_list(pipeline_context, instrument, frame_type, min_date, max_date, use_masters=False):
    return dbs.get_individual_calibration_images(instrument, frame_type, min_date, max_date,
                                                 use_masters=use_masters, db_address=pipeline_context.db_address)


def check_image_homogeneity(images, group_by_attributes=None):
    attribute_list = ['nx', 'ny', 'ccdsum', 'epoch', 'site', 'camera']
    if group_by_attributes is not None:
        attribute_list += group_by_attributes
    for attribute in attribute_list:
        if len(set([getattr(image, attribute) for image in images])) > 1:
            raise InhomogeneousSetException('Images have different {0}s'.format(attribute))


class InhomogeneousSetException(Exception):
    pass


class MissingCatalogException(Exception):
    pass
