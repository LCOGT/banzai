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
        super(Catalog, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for i, image in enumerate(images):
            self.logger.info('Extracting sources from {filename}'.format(filename=image.filename))
            data = image.data.copy()
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
