from __future__ import absolute_import, print_function, division

from astropy.io import fits
import numpy as np
import os.path

from .utils import stats, fits_utils
from . import dbs
from . import logs
from .stages import MakeCalibrationImage, ApplyCalibration

__author__ = 'cmccully'


class MakeDark(MakeCalibrationImage):
    def __init__(self, pipeline_context):

        super(MakeDark, self).__init__(pipeline_context,
                                       initial_query=pipeline_context.main_query,
                                       cal_type='dark', previous_stage_done=dbs.Image.trim_done,
                                       previous_suffix_number='15')
        self.log_message = 'Creating {binning} dark frame for {instrument} on {epoch}.'
        self.group_by = [dbs.Image.ccdsum]

    def do_stage(self, image_list, output_file, min_images=5, clobber=True):

        logger = logs.get_logger('Dark')
        if len(image_list) < min_images:
            logger.warning('Not enough images to combine.')
        else:
            # Assume the files are all the same number of pixels
            # TODO: add error checking for incorrectly sized images

            nx = image_list[0].naxis1
            ny = image_list[0].naxis2
            dark_data = np.zeros((ny, nx, len(image_list)))

            for i, image in enumerate(image_list):
                image_file = os.path.join(image.filepath, image.filename)
                image_file += self.previous_image_suffix + '.fits'
                image_data = fits.getdata(image_file)
                logger.debug('Combining dark {filename}'.format(filename=image.filename))

                dark_data[:, :, i] = image_data / image.exptime

            master_dark = stats.sigma_clipped_mean(dark_data, 3.0, axis=2)

            # Save the master dark image with all of the combined images in the header

            header = fits.Header()
            header['CCDSUM'] = image_list[0].ccdsum
            header['DAY-OBS'] = str(image_list[0].dayobs)
            header['CALTYPE'] = 'DARK'

            header.add_history("Images combined to create master dark image:")
            for image in image_list:
                header.add_history(image.filename)

            fits.writeto(output_file, master_dark, header=header, clobber=clobber)

            self.save_calibration_info('dark', output_file, image_list[0])


class SubtractDark(ApplyCalibration):
    def __init__(self, pipeline_context):

        dark_query = pipeline_context.main_query & (dbs.Image.obstype.in_(('SKYFLAT', 'EXPOSE')))

        super(SubtractDark, self).__init__(pipeline_context,
                                           initial_query=dark_query, cal_type='dark',
                                           previous_stage_done=dbs.Image.trim_done, previous_suffix_number='15',
                                           image_suffix_number='20')
        self.log_message = 'Subtracting {binning} dark frame for {instrument} on {epoch}.'
        self.group_by = [dbs.Image.ccdsum]

    def do_stage(self, image_files, output_files, master_dark_file, clobber=True):

        master_dark_data = fits.getdata(master_dark_file)

        logger = logs.get_logger('Dark')

        db_session = dbs.get_session()
        # TODO Add error checking for incorrect image sizes
        for i, image in enumerate(image_files):
            logger.debug('Subtracting dark for {image}'.format(image=image.filename))
            image_file = os.path.join(image.filepath, image.filename)
            image_file += self.previous_image_suffix + '.fits'
            data = fits.getdata(image_file)
            header = fits_utils.sanitizeheader(fits.getheader(image_file))

            data -= master_dark_data * image.exptime

            master_dark_filename = os.path.basename(master_dark_file)
            header.add_history('Master Dark: {dark_file}'.format(dark_file=master_dark_filename))
            output_filename = os.path.join(output_files[i].filepath, output_files[i].filename)
            output_filename += self.image_suffix_number + '.fits'
            fits.writeto(output_filename, data, header=header, clobber=clobber)

            image.dark_done = True
            db_session.add(image)
            db_session.commit()
        db_session.close()