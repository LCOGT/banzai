import os
from glob import glob
import logging
from datetime import timedelta

from astropy.io import fits

import banzai
from banzai import logs
from banzai import dbs
from banzai.utils import file_utils, fits_utils, date_utils

logger = logging.getLogger(__name__)


def image_passes_criteria(filename, criteria, db_address=dbs._DEFAULT_DB):
    instrument = dbs.get_instrument_for_file(filename, db_address=db_address)
    passes = True
    for criterion in criteria:
        if not criterion.instrument_passes(instrument):
            passes = False
    return passes


def image_is_correct_obstype(filename, image_types):
    obstype = None
    hdu_list = fits.open(filename)
    for hdu in hdu_list:
        if 'OBSTYPE' in hdu.header.keys():
            obstype = hdu.header['OBSTYPE']
    passes = True if obstype in image_types else False
    if obstype is None:
        logger.error('Unable to get OBSTYPE', extra={'tags': {'filename': filename}})
    return passes


def select_images(image_path_list, image_types, instrument_criteria, db_address=dbs._DEFAULT_DB):
    logger.info("Selecting images to reduce from list of filenames")
    pruned_image_path_list = []
    for filename in image_path_list:
        try:
            if image_passes_criteria(filename, instrument_criteria, db_address=db_address) and \
               image_is_correct_obstype(filename, image_types):
                pruned_image_path_list.append(filename)
        except Exception as e:
            logger.error('Exception checking image selection criteria: {e}'.format(e=e),
                         extra_tags={'filename': filename})
    logger.debug("Found {n_images} images to reduce".format(n_images=len(pruned_image_path_list)))
    return pruned_image_path_list


def get_calibration_image_parameters(filename, group_by_attributes, db_address):
    telescope = dbs.get_telescope_for_file(filename, db_address=db_address)
    _, header, _, _ = fits_utils.open_image(filename)
    image_parameters = {
        'dayobs': header['DAY-OBS'],
        'obstype': header['OBSTYPE'],
        'telescope_id': telescope.id}
    for attribute in group_by_attributes:
        # Works for CCDSUM and FILTER
        image_parameters[attribute] = header[attribute.upper()]
    return image_parameters


def select_calibration_images(image_path_list, image_types, instrument_criteria, group_by_attributes,
                              db_address=dbs._DEFAULT_DB):
    logger.info("Selecting individual reduced calibration images from database")
    # Using the raw image path list, check that each image file passes the criteria and is the correct obstype.
    # If so, record the parameters needed to query the db if they don't already exist in the list.
    image_parameters_list = []
    for filename in image_path_list:
        try:
            if image_passes_criteria(filename, instrument_criteria, db_address=db_address) and \
               image_is_correct_obstype(filename, image_types):
                image_parameters = get_calibration_image_parameters(filename, group_by_attributes,
                                                                    db_address=db_address)
                if image_parameters not in image_parameters_list:
                    image_parameters_list.append(image_parameters)
        except Exception:
            logger.error('Exception checking image selection criteria: ' + logs.format_exception(),
                         extra={'tags': {'filename': filename}})
    logger.debug("Checked {n_images} images, and found {n_parameter_sets} sets of parameters".format(
        n_images=len(image_path_list), n_parameter_sets=len(image_parameters_list)))
    # For each unique set of image parameters, query the db for reduced image files that match.
    # Return a list of reduced image filenames for each set of image parameters.
    calibration_image_path_lists = []
    for image_parameters in image_parameters_list:
        calibration_image_path_lists.append(
            dbs.get_individual_calibration_images(image_parameters,
                                                  group_by_attributes=group_by_attributes,
                                                  db_address=db_address))
    logger.debug("Returning {n_lists} image path lists, with respective lengths {len_lists}".format(
        n_lists=len(calibration_image_path_lists),
        len_lists=[len(image_path_list) for image_path_list in calibration_image_path_lists]
    ))
    return calibration_image_path_lists


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
        if image.obstype in pipeline_context.CALIBRATION_IMAGE_TYPES:
            dbs.save_calibration_info(image.obstype, filepath, image,
                                      db_address=pipeline_context.db_address, is_master=master_calibration)

        elif image.obstype in CALIBRATION_OBSTYPES:
            dbs.save_individual_calibration_info(image.obstype, filepath, image,
                                                 db_address=pipeline_context.db_address)
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
