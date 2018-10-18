import os
import logging
import tempfile
import shutil

import numpy as np
from astropy.io import fits
from astropy.table import Table

from banzai import dbs
from banzai.utils import date_utils
from banzai.utils import fits_utils
from banzai.utils import image_utils
from banzai.munge import munge

logger = logging.getLogger(__name__)


class DataTable(object):
    """
    Object for storing astropy (or another table type) tables with an additional .name attribute which
    determines the tables' extension name when it is saved as a fits file.
    """
    def __init__(self, data_table, name):
        self.name = name
        self._data_table = data_table

    def __getitem__(self, item):
        return self._data_table[item]

    def __setitem__(self, key, value):
        self._data_table[key] = value

    def table_to_hdu(self):
        table_hdu = fits_utils.table_to_fits(self._data_table)
        table_hdu.name = self.name
        return table_hdu


class Image(object):

    def __init__(self, pipeline_context, filename=None, data=None, data_tables=None,
                 header=None, extension_headers=None, bpm=None):
        if header is None:
            header = {}

        if data_tables is None:
            data_tables = {}

        if extension_headers is None:
            extension_headers = []

        if filename is not None:
            data, header, bpm, extension_headers = fits_utils.open_image(filename)
            if '.fz' == filename[-3:]:
                filename = filename[:-3]
            self.filename = os.path.basename(filename)

        self.data = data
        self.data_tables = data_tables
        self.header = header
        self.bpm = bpm

        self.extension_headers = extension_headers

        self.request_number = header.get('REQNUM')
        self.telescope, self.site, self.instrument = self._init_telescope_info(pipeline_context)

        self.epoch = str(header.get('DAY-OBS'))
        self.nx = header.get('NAXIS1')
        self.ny = header.get('NAXIS2')
        self.block_id = header.get('BLKUID')
        self.molecule_id = header.get('MOLUID')

        if len(self.extension_headers) > 0 and 'GAIN' in self.extension_headers[0]:
                self.gain = [h['GAIN'] for h in extension_headers]
        else:
            self.gain = eval(str(header.get('GAIN')))

        self.ccdsum = header.get('CCDSUM')
        self.filter = header.get('FILTER')

        self.obstype = header.get('OBSTYPE')
        self.exptime = float(header.get('EXPTIME', 0.0))
        self.dateobs = date_utils.parse_date_obs(header.get('DATE-OBS', '1900-01-01T00:00:00.00000'))
        self.readnoise = float(header.get('RDNOISE', 0.0))
        self.ra, self.dec = fits_utils.parse_ra_dec(header)
        self.pixel_scale = float(header.get('PIXSCALE', 0.0))

    def _init_telescope_info(self, pipeline_context):
        if len(self.header) > 0:
            telescope = dbs.get_telescope(self.header, db_address=pipeline_context.db_address)
            if telescope is not None:
                site = telescope.site
                instrument = telescope.instrument
            else:
                site = self.header.get('SITEID')
                instrument = self.header.get('INSTRUME')
        else:
            telescope, site, instrument = None, None, None
        return telescope, site, instrument

    def subtract(self, value):
        self.data -= value

    def writeto(self, filename, fpack=False):
        image_hdu = fits.PrimaryHDU(self.data.astype(np.float32), header=self.header)
        image_hdu.header['BITPIX'] = -32
        image_hdu.header['BSCALE'] = 1.0
        image_hdu.header['BZERO'] = 0.0
        image_hdu.header['SIMPLE'] = True
        image_hdu.header['EXTEND'] = True
        image_hdu.name = 'SCI'
        hdu_list = [image_hdu]
        hdu_list = self._add_data_tables_to_hdu_list(hdu_list)
        hdu_list = self._add_bpm_to_hdu_list(hdu_list)

        fits_hdu_list = fits.HDUList(hdu_list)
        try:
            fits_hdu_list.verify(option='exception')
        except fits.VerifyError as fits_error:
            logger.warning('Error in FITS Verification. {0}. Attempting fix.'.format(fits_error), image=self)
            try:
                fits_hdu_list.verify(option='silentfix+exception')
            except fits.VerifyError as fix_attempt_error:
                logger.error('Could not repair FITS header. {0}'.format(fix_attempt_error), image=self)

        with tempfile.TemporaryDirectory() as temp_directory:
            base_filename = os.path.basename(filename)
            fits_hdu_list.writeto(os.path.join(temp_directory, base_filename), overwrite=True,
                             output_verify='fix+warn')
            if fpack:
                filename += '.fz'
                if os.path.exists(filename):
                    os.remove(filename)
                os.system('fpack -q 64 {temp_directory}/{basename}'.format(temp_directory=temp_directory,
                                                                           basename=base_filename))
                base_filename += '.fz'
                self.filename += '.fz'
            shutil.move(os.path.join(temp_directory, base_filename), filename)

    def _add_data_tables_to_hdu_list(self, hdu_list):
        """
        :param hdu_list: a list of hdu objects.
        :return: a list of hdu objects with a FitsBinTableHDU added
        """
        for key in self.data_tables:
            table_hdu = self.data_tables[key].table_to_hdu()
            hdu_list.append(table_hdu)
        return hdu_list

    def _add_bpm_to_hdu_list(self, hdu_list):
        if self.bpm is not None:
            bpm_hdu = fits.ImageHDU(self.bpm.astype(np.uint8))
            bpm_hdu.name = 'BPM'
            hdu_list.append(bpm_hdu)
        return hdu_list

    def update_shape(self, nx, ny):
        self.nx = nx
        self.ny = ny

    def write_catalog(self, filename, nsources=None):
        if self.data_tables.get('catalog') is None:
            raise image_utils.MissingCatalogException
        else:
            self.data_tables.get('catalog')[:nsources].write(filename, format='fits', overwrite=True)

    def add_history(self, msg):
        self.header.add_history(msg)

    def data_is_3d(self):
        return len(self.data.shape) > 2

    def get_n_amps(self):
        if self.data_is_3d():
            n_amps = self.data.shape[0]
        else:
            n_amps = 1
        return n_amps

    def get_inner_image_section(self, inner_edge_width=0.25):
        """
        Extract the inner section of the image with dimensions:
        ny * inner_edge_width * 2.0 x nx * inner_edge_width * 2.0

        Parameters
        ----------

        inner_edge_width: float
                          Size of inner edge as fraction of total image size

        Returns
        -------
        inner_section: array
                       Inner section of image
        """
        if self.data_is_3d():
            logger.error("Cannot get inner section of a 3D image", image=self)
            raise ValueError

        inner_nx = round(self.nx * inner_edge_width)
        inner_ny = round(self.ny * inner_edge_width)
        return self.data[inner_ny: -inner_ny, inner_nx: -inner_nx]


def read_images(image_list, pipeline_context):
    images = []
    for filename in image_list:
        try:
            image = Image(pipeline_context, filename=filename)
            if image.telescope is None:
                error_message = 'Telescope is not in the database: {site}/{instrument}'
                error_message = error_message.format(site=image.site, instrument=image.instrument)
                raise dbs.TelescopeMissingException(error_message)
            munge(image, pipeline_context)
            if image.bpm is None:
                image_utils.load_bpm(image, pipeline_context)
            images.append(image)
        except Exception as e:
            logger.error('Error loading image: {error}'.format(error=e), extra_tags={'filename': filename})
            continue
    return images


def regenerate_data_table_from_fits_hdu_list(hdu_list, table_extension_name, input_dictionary=None):
    """
    :param hdu_list: An Astropy HDUList object
    :param table_extension_name: the name such that hdu_list[extension_name] = the table
    :param input_dictionary: the dictionary to which you wish to append the table under the keyword
            extension name.
    :return: the input_dictionary with dict[extension_name] = the table as an astropy table
    """
    if input_dictionary is None:
        input_dictionary = {}
    astropy_table = Table(hdu_list[table_extension_name].data)
    input_dictionary[table_extension_name] = astropy_table
    return input_dictionary
