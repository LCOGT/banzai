from __future__ import absolute_import, print_function, division
import os

from astropy.io import fits
from astropy.table import Table

from .stages import Stage
from . import dbs, logs
import sep

__author__ = 'cmccully'


class Catalog(Stage):
    def __init__(self, pipeline_context):

        catalog_query = pipeline_context.main_query & (dbs.Image.obstype == 'EXPOSE')

        super(Catalog, self).__init__(pipeline_context, initial_query=catalog_query,
                                      cal_type='catalog', previous_stage_done=dbs.Image.wcs_done,
                                      previous_suffix_number='90')
        self.group_by = None


    def do_stage(self, image_files, clobber=True):
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
            output_file = os.path.join(image.filepath, image.filename)
            output_file += self.image_suffix_number + '.cat'
            source_table.write(output_file, format='ascii')
