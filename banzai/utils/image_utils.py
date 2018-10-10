import os
from glob import glob
import logging
from datetime import timedelta

from astropy.io import fits

import banzai
from banzai import dbs
from banzai.utils import file_utils, fits_utils, date_utils

logger = logging.getLogger(__name__)

CALIBRATION_OBSTYPES = ['BIAS', 'DARK', 'SKYFLAT']


def image_passes_criteria(filename, criteria, db_address=dbs._DEFAULT_DB):
    telescope = dbs.get_telescope_for_file(filename, db_address=db_address)
    passes = True
    for criterion in criteria:
        if not criterion.telescope_passes(telescope):
            passes = False
    return passes


def _image_is_correct_obstype(filename, image_types):
    passes = False
    obstype = None
    hdu_list = fits.open(filename)
    for hdu in hdu_list:
        if 'OBSTYPE' in hdu.header.keys():
            obstype = hdu.header['OBSTYPE']
    if obstype is None:
        logger.error('Unable to get OBSTYPE', extra={'tags': {'filename': filename}})
        passes = False
    elif obstype in image_types:
        passes = True
    return passes


def _get_calibration_image_parameters(filename):
    telescope = dbs.get_telescope_for_file(filename, db_address=db_address)
    _, header, _, _ = fits_utils.open_image(filename)
    image_parameters = {
        'ccdsum': header['CCDSUM'],
        'filter': header['FILTER'],
        'dayobs': header['DAY-OBS'],
        'obstype': header['OBSTYPE'],
        'telescope_id': telescope.id}
    return image_parameters


def select_calibration_images(image_list, image_types, instrument_criteria, db_address=dbs._DEFAULT_DB):
    images = []
    image_parameters_list = []
    for filename in image_list:
        try:
            if image_passes_criteria(filename, instrument_criteria, db_address=db_address) and \
               _image_is_correct_obstype(filename, image_types):
                image_parameters = _get_calibration_image_parameters(filename)
                if image_parameters not in image_parameters_list:
                    image_parameters_list.append(image_parameters)
        except Exception as e:
            logger.error('Exception checking image selection criteria: {e}'.format(e=e),
                         extra={'tags': {'filename': filename}})
    for image_parameters in image_parameters_list:
        images.append(dbs.get_individual_calibration_images(image_parameters, db_address=db_address))
    return images


def select_images(image_list, image_types, instrument_criteria, db_address=dbs._DEFAULT_DB):
    images = []
    for filename in image_list:
        try:
            if image_passes_criteria(filename, instrument_criteria, db_address=db_address) and \
               _image_is_correct_obstype(filename, image_types):
                images.append(filename)
        except Exception as e:
            logger.error('Exception checking image selection criteria: {e}'.format(e=e),
                         extra_tags={'filename': filename})
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
    attribute_list = ['nx', 'ny', 'ccdsum', 'epoch', 'site', 'instrument']
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
        if master_calibration:
            dbs.save_calibration_info(image.obstype, filepath, image,
                                      db_address=pipeline_context.db_address)

        if image.obstype in CALIBRATION_OBSTYPES:
            dbs.save_individual_calibration_info(image.obstype, filepath, image,
                                                 db_address=pipeline_context.db_address)
        if pipeline_context.post_to_archive:
            logger.info('Posting {filename} to the archive'.format(filename=image_filename))
            try:
                file_utils.post_to_archive_queue(filepath)
            except Exception as e:
                logger.error("Could not post {0} to ingester.".format(filepath))
                logger.error(e)
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
