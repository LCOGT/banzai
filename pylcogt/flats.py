from __future__ import absolute_import, print_function, division

from astropy.io import fits
import numpy as np
import os.path

from .utils import stats, fits_utils
from . import dbs
from . import logs
from .stages import CalibrationMaker, ApplyCalibration
from pylcogt.utils.file_utils import post_to_archive_queue

__author__ = 'cmccully'


class FlatMaker(CalibrationMaker):

    min_images = 5

    def __init__(self, pipeline_context):

        super(FlatMaker, self).__init__(pipeline_context)

    @property
    def calibration_type(self):
        return 'flat'
    @property
    def group_by_keywords(self):
        return ['ccdsum', 'filter']

    def do_stage(self, images):

        if len(images) < self.min_images:
            self.logger.warning('Not enough images to combine.')
        else:
            master_flat_filename = self.get_calibration_filename(images[0])
            # Assume the files are all the same number of pixels
            # TODO: add error checking for incorrectly sized images

            flat_data = np.zeros((images[0].ny, images[0].nx, len(images)))

            for i, image in enumerate(images):

                flat_normalization = stats.mode(image.data)
                flat_data[:, :, i] = image.data / flat_normalization
                self.logger.debug('Calculating mode of {image}: mode = {mode}'.format(image=image.filename, mode=flat_normalization))
            master_flat = stats.sigma_clipped_mean(flat_data, 3.0, axis=2)

            # Save the master flat field image with all of the combined images in the header

            header = fits.Header()
            header['CCDSUM'] = images[0].ccdsum
            header['DAY-OBS'] = str(images[0].epoch)
            header['CALTYPE'] = 'SKYFLAT'

            header.add_history("Images combined to create master flat field image:")
            for image in images:
                header.add_history(image.filename)

            fits.writeto(master_flat_filename, master_flat, header=header, clobber=True)
            post_to_archive_queue(master_flat_filename)
            dbs.save_calibration_info('skyflat', master_flat_filename, images[0])

        return images


class FlatDivider(ApplyCalibration):
    def __init__(self, pipeline_context):

        super(FlatDivider, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return ['ccdsum', 'filter']

    @property
    def calibration_type(self):
        return 'flat'

    def do_stage(self, images):
        master_flat_filename = self.get_calibration_filename(images[0])
        if master_flat_filename is None:
            self.logger.warning('No flatfield for this image configuration')
            return images

        master_flat_data = fits.getdata(master_flat_filename)

        # TODO Add error checking for incorrect image sizes
        for image in images:
            self.logger.debug('Flattening {image}'.format(image=image.filename))

            image.data /= master_flat_data

            master_flat_filename = os.path.basename(master_flat_file)
            header.add_history('Master Flat: {flat_file}'.format(flat_file=master_flat_filename))

        return images