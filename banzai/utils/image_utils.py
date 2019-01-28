import os
from glob import glob
import logging
from datetime import timedelta

from astropy.io import fits

from banzai import logs
from banzai import dbs
from banzai.utils import date_utils

logger = logging.getLogger(__name__)


def image_passes_criteria(filename, criteria, db_address=dbs._DEFAULT_DB):
    instrument = dbs.get_instrument_for_file(filename, db_address=db_address)
    return dbs.instrument_passes_criteria(instrument, criteria)


def get_obstype(filename):
    obstype = None
    hdu_list = fits.open(filename)
    for hdu in hdu_list:
        if 'OBSTYPE' in hdu.header.keys():
            obstype = hdu.header['OBSTYPE']

    if obstype is None:
        logger.error('Unable to get OBSTYPE', extra_tags={'filename': filename})

    return obstype


def get_obstype(filename):
    obstype = None
    hdu_list = fits.open(filename)
    for hdu in hdu_list:
        if 'OBSTYPE' in hdu.header.keys():
            obstype = hdu.header['OBSTYPE']
    if obstype is None:
        logger.error('Unable to get OBSTYPE', extra_tags={'filename': filename})
    return obstype


def image_is_correct_type(filename, image_types):
    if image_types is None:
        return True
    return get_obstype(filename) in image_types


def select_images(image_list, instrument_criteria, image_types=None, db_address=dbs._DEFAULT_DB):
    images = []
    for filename in image_list:
        try:
            if image_passes_criteria(filename, instrument_criteria, db_address=db_address) and \
                    image_is_correct_type(filename, image_types):
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


def get_grouped_calibration_image_path_lists(pipeline_context, instrument, dayobs, frame_type, use_masters=False):
    timezone = dbs.get_timezone(instrument.site, db_address=pipeline_context.db_address)
    midnight_at_site = date_utils.get_midnight(dayobs, timezone)
    date_range = (midnight_at_site - timedelta(days=(0.5 + pipeline_context.CALIBRATION_DAYS_TO_STACK[frame_type]-1)),
                  midnight_at_site + timedelta(days=0.5))

    return dbs.get_individual_calibration_images(instrument, date_range, frame_type,
                                                 pipeline_context.CALIBRATION_SET_CRITERIA[frame_type],
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
