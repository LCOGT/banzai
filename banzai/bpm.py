import os

import numpy as np

from banzai.stages import Stage
from banzai.utils import array_utils, fits_utils
from banzai import dbs


class BPMUpdater(Stage):
    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for image in images:
            self.add_bpm_to_image(image)
            bpm_slices = array_utils.array_indices_to_slices(image.bpm)
            image.bpm[image.data[bpm_slices] >= float(image.header['SATURATE'])] = 2
        return images

    def add_bpm_to_image(self, image):
        # Get the BPM filename
        bpm_filename = dbs.get_bpm(image.telescope.id, image.ccdsum,
                                   db_address=self.pipeline_context.db_address)
        # Check if file is missing
        is_bpm_missing = bpm_filename is None
        self.save_qc_results({'pipeline.bpm.missing': is_bpm_missing}, image)
        if is_bpm_missing:
            # Short circuit if BPM is missing
            self.logger.warning('Missing BPM for {0}'.format(image.filename))
            return

        # Load the BPM
        self.logger.debug('Loading BPM {0}'.format(bpm_filename))
        bpm = load_bpm(bpm_filename, self.logger)

        # Check if the BPM is the right size
        is_invalid_size = not bpm_has_valid_size(bpm, image)
        self.save_qc_results({'pipeline.bpm.invalid_size': is_invalid_size}, image)
        if is_invalid_size:
            # Short circuit if BPM is the wrong size
            self.logger.warning('BPM shape mismatch for {0}'.format(image.filename))
            return

        # Add BPM to image and header info
        image.bpm = bpm
        image.header['L1IDMASK'] = (os.path.basename(bpm_filename), 'Id. of mask file used')


def load_bpm(bpm_filename, logger):
    bpm_hdu = fits_utils.open_fits_file(bpm_filename)
    bpm_extensions = fits_utils.get_extensions_by_name(bpm_hdu, 'BPM')
    logger.info('bpm_extensions {0}'.format(len(bpm_extensions)))
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
