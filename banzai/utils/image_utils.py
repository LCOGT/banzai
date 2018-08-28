from __future__ import absolute_import, division, print_function, unicode_literals
import os
from glob import glob

from astropy.io import fits
import numpy as np

import banzai
from banzai import dbs
from banzai import logs
from banzai.utils import file_utils
from banzai.utils import fits_utils
from banzai.utils import date_utils
from datetime import timedelta

logger = logs.get_logger(__name__)


class MissingBPMError(Exception):
    pass


def select_images(image_list, image_types):
    images = []
    for filename in image_list:
        try:
            obstype = None
            hdu_list = fits.open(filename)
            for hdu in hdu_list:
                if 'OBSTYPE' in hdu.header.keys():
                    obstype = hdu.header['OBSTYPE']

            if obstype is None:
                logger.error('Unable to get OBSTYPE', extra={'tags': {'filename': filename}})

            if obstype in image_types:
                images.append(filename)
        except Exception as e:
            logger.error('Exception getting OBSTYPE: {e}'.format(e=e),
                         extra={'tags': {'filename': filename}})
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
                fz_files_to_remove.append(f)

        for f in fz_files_to_remove:
            fz_files.remove(f)
        image_list = fits_files + fz_files

    else:
        image_list = glob(os.path.join(pipeline_context.raw_path, pipeline_context.filename))
    return image_list


def check_image_homogeneity(images):
    for attribute in ['nx', 'ny', 'ccdsum', 'epoch', 'site', 'instrument']:
        if len(set([getattr(image, attribute) for image in images])) > 1:
            raise InhomogeneousSetException('Images have different {0}s'.format(attribute))
    return images[0]


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

        if pipeline_context.post_to_archive:
            logger.info('Posting {filename} to the archive'.format(filename=image_filename))
            try:
                file_utils.post_to_archive_queue(filepath)
            except Exception as e:
                logger.error("Could not post {0} to ingester.".format(filepath))
                logger.error(e)
                continue
    return output_files


def load_bpm(image, pipeline_context):
    bpm_filename = dbs.get_bpm(image.telescope.id, image.ccdsum,
                               db_address=pipeline_context.db_address)
    if pipeline_context.no_bpm:
        load_empty_bpm(image)
    elif bpm_filename is None:
        raise MissingBPMError('No Bad Pixel Mask file exists for this image.')
    else:
        load_bpm_file(bpm_filename, image)


def load_empty_bpm(image):
    if image.data is None:
        image.bpm = None
    else:
        image.bpm = np.zeros(image.data.shape, dtype=np.uint8)
    image.header['L1IDMASK'] = ('', 'Id. of mask file used')


def load_bpm_file(bpm_filename, image):
    bpm_hdu = fits_utils.open_fits_file(bpm_filename)
    bpm_extensions = fits_utils.get_extensions_by_name(bpm_hdu, 'BPM')
    if len(bpm_extensions) > 1:
        extension_shape = bpm_extensions[0].data.shape
        bpm_shape = (len(bpm_extensions), extension_shape[0], extension_shape[1])
        image.bpm = np.zeros(bpm_shape, dtype=np.uint8)
        for i, extension in enumerate(bpm_extensions):
            image.bpm[i, :, :] = extension.data[:, :]
    elif len(bpm_extensions) == 1:
        image.bpm = np.array(bpm_extensions[0].data, dtype=np.uint8)
    else:
        image.bpm = np.array(bpm_hdu[0].data, dtype=np.uint8)
    if not bpm_has_valid_size(image.bpm, image):
        tags = logs.image_config_to_tags(image, None)
        logs.add_tag(tags, 'filename', image.filename)
        logger.error('BPM shape mismatch', extra=tags)
        raise ValueError('BPM shape mismatch')
    image.header['L1IDMASK'] = (os.path.basename(bpm_filename), 'Id. of mask file used')


def bpm_has_valid_size(bpm, image):

    # If 3d, check and make sure the number of extensions is the same
    if image.data_is_3d():
        y_slices, x_slices = fits_utils.parse_region_keyword(image.extension_headers[0]['DATASEC'])
        is_valid = image.data.shape[0] == bpm.shape[0]
    else:
        y_slices, x_slices = fits_utils.parse_region_keyword(image.header['DATASEC'])
        is_valid = True

    # Check if x and y dimensions are less than the datasec
    is_valid &= bpm.shape[-1] >= x_slices.stop
    is_valid &= bpm.shape[-2] >= y_slices.stop

    return is_valid


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
    logging_tags = logs.image_config_to_tags(image, None)
    logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
    logs.add_tag(logging_tags, 'rlevel', int(image.header['RLEVEL']))
    logs.add_tag(logging_tags, 'pipeline_version', image.header['PIPEVER'])
    logs.add_tag(logging_tags, 'l1pubdat', image.header['L1PUBDAT'])
    logger.info('Updating header', extra=logging_tags)
