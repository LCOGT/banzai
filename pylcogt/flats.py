from __future__ import absolute_import, print_function, division

from astropy.io import fits
import numpy as np
import os.path

from .utils import stats, fits_utils
from . import dbs
from . import logs
from .stages import MakeCalibrationImage, ApplyCalibration

__author__ = 'cmccully'


class MakeFlat(MakeCalibrationImage):
    def __init__(self, pipeline_context, initial_query):

        super(MakeFlat, self).__init__(pipeline_context,
                                       initial_query=initial_query, previous_stage_done=dbs.Image.dark_done,
                                       cal_type='skyflat', previous_suffix_number='20')
        self.log_message = 'Creating master flat-field frame'
        self.group_by = [dbs.Image.ccdsum, dbs.Image.filter_name]

    def do_stage(self, image_list, output_file, min_images=5, clobber=True):

        logger = logs.get_logger('Flat')
        if len(image_list) < min_images:
            logger.warning('Not enough images to combine.')
        else:
            # Assume the files are all the same number of pixels
            # TODO: add error checking for incorrectly sized images

            nx = image_list[0].naxis1
            ny = image_list[0].naxis2
            flat_data = np.zeros((ny, nx, len(image_list)))

            for i, image in enumerate(image_list):
                image_file = os.path.join(image.filepath, image.filename)
                image_file += self.previous_image_suffix + '.fits'
                image_data = fits.getdata(image_file)

                flat_normalization = stats.mode(image_data)
                flat_data[:, :, i] = image_data / flat_normalization
                logger.debug('Calculating mode of {image}: mode = {mode}'.format(image=image.filename, mode=flat_normalization))
            master_flat = stats.sigma_clipped_mean(flat_data, 3.0, axis=2)

            # Save the master flat field image with all of the combined images in the header

            header = fits.Header()
            header['CCDSUM'] = image_list[0].ccdsum
            header['DAY-OBS'] = str(image_list[0].dayobs)
            header['CALTYPE'] = 'SKYFLAT'

            header.add_history("Images combined to create master flat field image:")
            for image in image_list:
                header.add_history(image.filename)

            fits.writeto(output_file, master_flat, header=header, clobber=clobber)

            self.save_calibration_info('skyflat', output_file, image_list[0])


class DivideFlat(ApplyCalibration):
    def __init__(self, pipeline_context, initial_query):

        flat_query = initial_query & (dbs.Image.obstype == 'EXPOSE')

        super(DivideFlat, self).__init__(pipeline_context,
                                         initial_query=flat_query, cal_type='skyflat',
                                         previous_stage_done=dbs.Image.dark_done,
                                         image_suffix_number='25', previous_suffix_number='20')
        self.log_message = 'Dividing master flat-field frame.'
        self.group_by = [dbs.Image.ccdsum, dbs.Image.filter_name]

    def do_stage(self, image_files, output_files, master_flat_file, clobber=True):

        master_flat_data = fits.getdata(master_flat_file)

        logger = logs.get_logger('Flat')

        db_session = dbs.get_session()
        # TODO Add error checking for incorrect image sizes
        for i, image in enumerate(image_files):
            logger.debug('Flattening {image}'.format(image=image.filename))
            image_file = os.path.join(image.filepath, image.filename)
            image_file += self.previous_image_suffix + '.fits'
            data = fits.getdata(image_file)
            header = fits_utils.sanitizeheader(fits.getheader(image_file))

            data /= master_flat_data

            master_flat_filename = os.path.basename(master_flat_file)
            header.add_history('Master Flat: {flat_file}'.format(flat_file=master_flat_filename))
            output_filename = os.path.join(output_files[i].filepath, output_files[i].filename)
            output_filename += self.image_suffix_number + '.fits'
            fits.writeto(output_filename, data, header=header, clobber=clobber)

            image.flat_done = True
            db_session.add(image)
            db_session.commit()
        db_session.close()
