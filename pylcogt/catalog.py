from __future__ import absolute_import, print_function, division
from .stages import Stage
from . import dbs, logs
from .utils import fits_utils
from astropy.io import fits, ascii
from astropy.table import Table
import os
import sep

__author__ = 'cmccully'

class Catalog(Stage):
    def __init__(self, raw_path, processed_path, initial_query, cpu_pool):

        catalog_query = initial_query & (dbs.Image.obstype == 'EXPOSE')

        super(Catalog, self).__init__(self.make_catalog, processed_path=processed_path, initial_query=catalog_query,
                                      logger_name='Catalog', cal_type='catalog', previous_stage_done=dbs.Image.wcs_done,
                                      previous_suffix_number='90', cpu_pool=cpu_pool)
        self.log_message = 'Generating source catalog for {instrument} at {site} on {epoch}.'
        self.group_by = None

    def get_output_images(self, telescope, epoch):
        image_sets, image_configs = self.select_input_images(telescope, epoch)
        image_list = [image for image_set in image_sets for image in image_set]
        return image_list

    def make_catalog(self, image_files, output_files, clobber=True):
        logger = logs.get_logger('Catalog')
        for i, image in enumerate(image_files):
            logger.debug('Extracting sources from {filename}'.format(filename=image.filename))
            image_file = os.path.join(image.filepath, image.filename)
            image_file += self.previous_image_suffix + '.fits'
            hdu = fits.open(image_file)
            data = hdu[0].data
            try:
                bkg = sep.Background(data)
            except ValueError:
                data = data.byteswap(True).newbyteorder()
                bkg = sep.Background(data)
            bkg.subfrom(data)
            threshold = 1.5 * bkg.globalrms

            sources = sep.extract(data, threshold)

            source_table = Table(sources[['x', 'y', 'flux', 'a', 'b', 'theta']],
                                 names=('X', 'Y', 'FLUX', 'a', 'b', 'THETA'))
            output_file = os.path.join(output_files[i].filepath, output_files[i].filename)
            output_file += self.image_suffix_number + '.cat'
            source_table.write(output_file, format='ascii')