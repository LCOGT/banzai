import os
import logging

import numpy as np

from banzai.stages import Stage
from banzai.utils import array_utils, fits_utils
from banzai import dbs

logger = logging.getLogger(__name__)


class BPMUpdater(Stage):

    def do_stage(self, images):
        images_to_remove = []
        for image in images:
            add_bpm_to_image(image, self.pipeline_context)
            validate_bpm_size(image)
            if image.header.get('L1IDMASK', '') == '' and not self.pipeline_context.no_bpm:
                logger.error("Can't add BPM to image, stopping reduction", image=image)
                images_to_remove.append(image)
                continue
            flag_bad_pixels(image)
            logger.info('Added BPM to image', image=image, extra_tags={'l1idmask': image.header['L1IDMASK']})
        for image in images_to_remove:
            images.remove(image)
        return images


def add_bpm_to_image(image, pipeline_context):
    # Exit if image already has a BPM
    if image.bpm is not None:
        return
    # Get the BPM filename
    bpm_filename = dbs.get_bpm_filename(image.instrument.id, image.ccdsum, db_address=pipeline_context.db_address)
    # Check if file is missing
    if bpm_filename is None:
        logger.warning('Unable to find BPM in database, falling back to empty BPM', image=image)
        add_empty_bpm(image)
        return
    # Load and add the BPM
    bpm = load_bpm(bpm_filename)
    set_image_bpm_and_header(image, bpm, os.path.basename(bpm_filename))


def add_empty_bpm(image):
    if image.data is None:
        bpm = None
    else:
        bpm = np.zeros(image.data.shape, dtype=np.uint8)
    set_image_bpm_and_header(image, bpm, '')


def load_bpm(bpm_filename):
    bpm_hdu = fits_utils.open_fits_file(bpm_filename)
    bpm_extensions = fits_utils.get_extensions_by_name(bpm_hdu, 'BPM')
    if len(bpm_extensions) > 1:
        extension_shape = bpm_extensions[0].data.shape
        bpm_shape = (len(bpm_extensions), extension_shape[0], extension_shape[1])
        bpm = np.zeros(bpm_shape, dtype=np.uint8)
        for i, extension in enumerate(bpm_extensions):
            bpm[i, :, :] = extension.data[:, :]
    elif len(bpm_extensions) == 1:
        bpm = np.array(bpm_extensions[0].data, dtype=np.uint8)
    else:
        bpm = np.array(bpm_hdu[0].data, dtype=np.uint8)
    return bpm


def set_image_bpm_and_header(image, bpm, bpm_filename):
    image.bpm = bpm
    image.header['L1IDMASK'] = (bpm_filename, 'Id. of mask file used')


def validate_bpm_size(image):
    if not bpm_has_valid_size(image):
        logger.warning('BPM shape mismatch, falling back to empty BPM', image=image)
        add_empty_bpm(image)


def bpm_has_valid_size(image):
    is_valid = True
    # If 3d, check and make sure the number of extensions is the same
    if image.data_is_3d():
        for i in range(image.get_n_amps()):
            is_valid &= image.bpm[i].shape == image.data[i].shape
    else:
        is_valid &= image.bpm.shape == image.data.shape
    return is_valid


def flag_bad_pixels(image):
    bpm_slices = array_utils.array_indices_to_slices(image.bpm)
    image.bpm[image.data[bpm_slices] >= float(image.header['SATURATE'])] = 2
