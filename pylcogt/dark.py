from __future__ import absolute_import, print_function, division

from astropy.io import fits
import numpy as np
import os.path

from .utils import stats, fits_utils
from . import dbs
from . import logs
from .stages import CalibrationMaker, ApplyCalibration

__author__ = 'cmccully'


class DarkMaker(CalibrationMaker):

    min_images = 5

    def __init__(self, pipeline_context):

        super(DarkMaker, self).__init__(pipeline_context)
        self.group_by = ['ccdsum']

    def do_stage(self, images):

        if len(images) < self.min_images:
            self.logger.warning('Not enough images to combine.')
        else:
            # Assume the files are all the same number of pixels
            # TODO: add error checking for incorrectly sized images

            dark_data = np.zeros((images[0].ny, images[0].nx, len(images)))

            for i, image in enumerate(images):
                self.logger.debug('Combining dark {filename}'.format(filename=image.filename))

                dark_data[:, :, i] = image.data / image.exptime

            master_dark = stats.sigma_clipped_mean(dark_data, 3.0, axis=2)

            # Save the master dark image with all of the combined images in the header

            header = fits.Header()
            header['CCDSUM'] = images[0].ccdsum
            header['DAY-OBS'] = str(images[0].dayobs)
            header['CALTYPE'] = 'DARK'

            header.add_history("Images combined to create master dark image:")
            for image in images:
                header.add_history(image.filename)

            master_dark_filename = self.get_calibration_filename(images[0])
            fits.writeto(master_dark_filename, master_dark, header=header, clobber=True)

            self.save_calibration_info('dark', master_dark_filename, images[0])
        return images


class DarkSubtractor(ApplyCalibration):
    def __init__(self, pipeline_context):
        super(DarkSubtractor, self).__init__(pipeline_context)
        self.group_by = ['ccdsum']

    def do_stage(self, images,):
        master_dark_file = self.get_calibration_filename(images[0])
        master_dark_filename = os.path.basename(master_dark_file)
        master_dark_data = fits.getdata(master_dark_file)

        # TODO Add error checking for incorrect image sizes
        for image in images:
            self.logger.debug('Subtracting dark for {image}'.format(image=image.filename))
            image.data -= master_dark_data * image.exptime
            image.header.add_history('Master Dark: {dark_file}'.format(dark_file=master_dark_filename))


        return images