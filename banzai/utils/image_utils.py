import os
from glob import glob
import logging
from datetime import timedelta

from astropy.io import fits

import banzai
from banzai import logs
from banzai import dbs
from banzai.utils import file_utils, date_utils

logger = logging.getLogger(__name__)

CALIBRATION_OBSTYPES = ["BIAS", "DARK", "SKYFLAT"]


def image_passes_criteria(filename, criteria, db_address=dbs._DEFAULT_DB):
    instrument = dbs.get_instrument_for_file(filename, db_address=db_address)
    passes = True
    for criterion in criteria:
        if not criterion.instrument_passes(instrument):
            passes = False
    return passes


def select_images(image_list, image_types, instrument_criteria, db_address=dbs._DEFAULT_DB):
    images = []
    for filename in image_list:
        try:
            if not image_passes_criteria(filename, instrument_criteria, db_address=db_address):
                continue
            obstype = None
            hdu_list = fits.open(filename)
            for hdu in hdu_list:
                if 'OBSTYPE' in hdu.header.keys():
                    obstype = hdu.header['OBSTYPE']

            if obstype is None:
                logger.error('Unable to get OBSTYPE', extra_tags={'filename': filename})

            if obstype in image_types:
                images.append(filename)
        except Exception:
            logger.error(logs.format_exception(), extra_tags={'filename': filename})
    return images


def make_image_list(raw_path):
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
        image_list = fits_files + fz_files

    else:
        image_list = glob(raw_path)
    return image_list


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


def save_images(pipeline_context, images, master_calibration=False):
    output_files = []
    for image in images:
        output_directory = file_utils.make_output_directory(pipeline_context, image)
        if not master_calibration:
            image.filename = image.filename.replace('00.fits',
                                                    '{:02d}.fits'.format(int(pipeline_context.rlevel)))

        image_filename = os.path.basename(image.filename)
        filepath = os.path.join(output_directory, image_filename)
        output_files.append(filepath)
        save_pipeline_metadata(image, pipeline_context)
        image.writeto(filepath, pipeline_context.fpack)
        if pipeline_context.fpack:
            image_filename += '.fz'
            filepath += '.fz'
        if master_calibration or image.obstype in CALIBRATION_OBSTYPES:
            dbs.save_calibration_info(image.obstype, filepath, image,
                                      db_address=pipeline_context.db_address, is_master=master_calibration)

        if pipeline_context.post_to_archive:
            logger.info('Posting file to the archive', extra_tags={'filename': image_filename})
            try:
                file_utils.post_to_archive_queue(filepath)
            except Exception:
                logger.error("Could not post to ingester: {error}".format(error=logs.format_exception()),
                             extra_tags={'filename': filepath})
                continue
    return output_files


def save_pipeline_metadata(image, pipeline_context):
    image.header['RLEVEL'] = (pipeline_context.rlevel, 'Reduction level')
    image.header['PIPEVER'] = (banzai.__version__, 'Pipeline version')

    if file_utils.instantly_public(image.header['PROPID']):
        image.header['L1PUBDAT'] = (image.header['DATE-OBS'],
                                    '[UTC] Date the frame becomes public')
    else:
        # Wait a year
        date_observed = date_utils.parse_date_obs(image.header['DATE-OBS'])
        next_year = date_observed + timedelta(days=365)
        image.header['L1PUBDAT'] = (date_utils.date_obs_to_string(next_year),
                                    '[UTC] Date the frame becomes public')
    logging_tags = {'rlevel': int(image.header['RLEVEL']),
                    'pipeline_version': image.header['PIPEVER'],
                    'l1pubdat': image.header['L1PUBDAT'],}
    logger.info('Updating header', image=image, extra_tags=logging_tags)
