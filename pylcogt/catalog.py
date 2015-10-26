__author__ = 'cmccully'
from __future__ import absolute_import, print_function, division
from .stages import Stage
from . import dbs, logs
from .utils import fits_utils
from astropy.io import fits
import os

__author__ = 'cmccully'

class Catalog(Stage):
    def __init__(self, raw_path, processed_path, initial_query):

        trim_query = initial_query & (dbs.Image.obstype.in_(('DARK', 'SKYFLAT', 'EXPOSE')))

        super(Catalog, self).__init__(self.trim, processed_path=processed_path,
                                   initial_query=trim_query, logger_name='Trim', cal_type='trim')
        self.log_message = 'Generating source catalog for {instrument} at {site} on {epoch}.'
        self.group_by = None

    def get_output_images(self, telescope, epoch):
        image_sets, image_configs = self.select_input_images(telescope, epoch)
        image_list = [image for image_set in image_sets for image in image_set]
        catalog_list = [image.replace('.fits', '.cat') for image in image_list]
        return catalog_list

    def make_catalog(self, image_files, output_files, clobber=True):
        logger = logs.get_logger('Catalog')
        for i, image in enumerate(image_files):
            image_file = os.path.join(image.filepath, image.filename)

