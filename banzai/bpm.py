import os
import logging

import numpy as np

from banzai.stages import Stage
from banzai.utils import array_utils, fits_utils
from banzai import dbs

logger = logging.getLogger(__name__)


class BPMUpdater(Stage):
    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        images_to_remove = []
        for image in images:
            self.add_bpm_to_image(image)
            if image.bpm is None:
                if self.pipeline_context.no_bpm:
                    add_empty_bpm(image)
                    logger.info('BPM misisng but not required, falling back to empty BPM', image=image)
                else:
                    logger.error("Can't add BPM to image, stopping reduction", image=image)
                    images_to_remove.append(image)
                    continue
            flag_bad_pixels(image)
        for image in images_to_remove:
            images.remove(image)
        return images

    def add_bpm_to_image(self, image):
        # Get the BPM filename
        bpm_filename = self.get_bpm_filename(image)
        # Check if file is missing
        if bpm_filename is None:
            logger.warning('Unable to find BPM in database', image=image)
            return
        # Load the BPM
        bpm = load_bpm(bpm_filename)
        # Check if the BPM is the right size
        if not bpm_has_valid_size(bpm, image):
            logger.warning('BPM shape mismatch', image=image)
            return
        # Add BPM to image and header info
        image.bpm = bpm
        image.header['L1IDMASK'] = (os.path.basename(bpm_filename), 'Id. of mask file used')
        logger.debug('Added BPM from file {}'.format(bpm_filename))

    def get_bpm_filename(self, image):
        return dbs.get_bpm_filename(image.telescope.id, image.ccdsum,
                                    db_address=self.pipeline_context.db_address)


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


def bpm_has_valid_size(bpm, image):
    is_valid = True
    # If 3d, check and make sure the number of extensions is the same
    if image.data_is_3d():
        for i in range(image.get_n_amps()):
            is_valid &= bpm[i].shape == image.data[i].shape
    else:
        is_valid &= bpm.shape == image.data.shape
    return is_valid


def add_empty_bpm(image):
    if image.data is None:
        image.bpm = None
    else:
        image.bpm = np.zeros(image.data.shape, dtype=np.uint8)
    image.header['L1IDMASK'] = ('', 'Id. of mask file used')


def flag_bad_pixels(image):
    bpm_slices = array_utils.array_indices_to_slices(image.bpm)
    image.bpm[image.data[bpm_slices] >= float(image.header['SATURATE'])] = 2
