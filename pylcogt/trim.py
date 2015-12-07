from __future__ import absolute_import, print_function, division

import itertools

from astropy.io import fits

from . import dbs, logs
from .utils import fits_utils
from pylcogt.stages import make_output_directory

__author__ = 'cmccully'

logger = logs.get_logger('Trim')


def _trim_image(hdu):
    trimsec = fits_utils.parse_region_keyword(hdu[0].header['TRIMSEC'])

    hdu[0].data = hdu[0].data[trimsec]
    # Update the NAXIS and CRPIX keywords
    hdu[0].header['NAXIS1'] = trimsec[1].stop - trimsec[1].start
    hdu[0].header['NAXIS2'] = trimsec[0].stop - trimsec[0].start
    hdu[0].header['CRPIX1'] -= trimsec[1].start
    hdu[0].header['CRPIX2'] -= trimsec[0].start

    return hdu[0].header['NAXIS1'], hdu[0].header['NAXIS2']


class Trim(object):
    def __init__(self, pipeline_context, initial_query):
        trim_query = initial_query & (dbs.Image.obstype.in_(('DARK', 'SKYFLAT', 'EXPOSE')))

        self.pipeline_context = pipeline_context
        self.initial_query = trim_query
        self.logger = logs.get_logger("Trim")
        self.cal_type = "trim"
        self.image_suffix_number = '15'
        self.previous_stage_done = dbs.Image.bias_done
        self.previous_image_suffix = '10'
        self.log_message = 'Trimming images from {instrument} at {site} on {epoch}.'

    def run(self, epoch_list, telescope_list):

        for epoch, telescope in itertools.product(epoch_list, telescope_list):
            make_output_directory(self.pipeline_context.processed_path, epoch, telescope)
            image_sets, _ = dbs.select_input_images(telescope, epoch, self.initial_query,
                                                    self.previous_stage_done, None)

            for image_set in image_sets:
                tags = logs.image_config_to_tags(image_set[0], telescope, epoch)
                self.logger.info(self.log_message, extra=tags)
                self.do_stage(image_set)

        return

    def do_stage(self, input_images):
        images_to_save = []
        for input_image in input_images:
            image_file = input_image.get_full_filename(self.previous_image_suffix)

            hdu = fits.open(image_file)

            naxis1, naxis2 = _trim_image(hdu)

            logger.debug('Trimming {image_name} to {trim_sec}'.format(image_name=input_image.filename,
                                                                      trim_sec=hdu[0].header['TRIMSEC']))

            output_filename = input_image.get_full_filename(self.image_suffix_number)
            hdu.writeto(output_filename, clobber=True)

            # Update the database
            input_image.naxis1 = naxis1
            input_image.naxis2 = naxis2
            input_image.trim_done = True

            images_to_save.append(input_image)

        dbs.save_images(images_to_save)
