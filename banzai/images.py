import os
import logging
import tempfile
import shutil
import datetime

import numpy as np
from astropy.io import fits
from astropy.table import Table, Column

import banzai
from banzai import dbs, settings, exceptions
from banzai.utils import date_utils, file_utils, fits_utils
from banzai import logs

logger = logging.getLogger('banzai')


class DataTable(object):
    """
    Object for storing astropy (or another table type) tables with an additional .name attribute which
    sets the table's extension name when it is saved as a fits file.
    """
    def __init__(self, data_table, name):
        self.name = name
        self._data_table = data_table

    def __getitem__(self, item):
        return self._data_table[item]

    def __setitem__(self, key, value):
        self._data_table[key] = value

    def add_column(self, arr, name, index=None):
        self._data_table.add_column(Column(arr), name=name, index=index)

    def table_to_hdu(self):
        table_hdu = fits_utils.table_to_fits(self._data_table)
        table_hdu.name = self.name
        return table_hdu


class Image(object):

    def __init__(self, runtime_context, filename=None, data=None, data_tables=None,
                 header=None, extension_headers=None, bpm=None):
        if header is None:
            header = fits.Header()

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
        self.instrument, self.site, self.camera = self._init_instrument_info(runtime_context)

        self.epoch = str(header.get('DAY-OBS'))
        self.nx = header.get('NAXIS1')
        self.ny = header.get('NAXIS2')
        self.block_id = header.get('BLKUID')
        self.block_start = date_utils.parse_date_obs(header.get('BLKSDATE', '1900-01-01T00:00:00.00000'))
        self.molecule_id = header.get('MOLUID')

        if len(self.extension_headers) > 0 and 'GAIN' in self.extension_headers[0]:
                self.gain = [h['GAIN'] for h in extension_headers]
        else:
            self.gain = eval(str(header.get('GAIN')))

        self.ccdsum = header.get('CCDSUM')
        self.configuration_mode = header.get('CONFMODE', 'default')

        # If the configuration mode is not in the header, fallback to default to support legacy data
        if (
                self.configuration_mode == 'N/A' or
                self.configuration_mode == 0 or
                self.configuration_mode.lower() == 'normal'
        ):
            self.configuration_mode = 'default'

        self.header['CONFMODE'] = self.configuration_mode
        self.filter = header.get('FILTER')

        self.obstype = header.get('OBSTYPE')
        self.exptime = float(header.get('EXPTIME', 0.0))
        self.dateobs = date_utils.parse_date_obs(header.get('DATE-OBS', '1900-01-01T00:00:00.00000'))
        self.datecreated = date_utils.parse_date_obs(header.get('DATE', date_utils.date_obs_to_string(self.dateobs)))
        self.readnoise = float(header.get('RDNOISE', 0.0))
        self.ra, self.dec = fits_utils.parse_ra_dec(header)
        self.pixel_scale = float(header.get('PIXSCALE', 0.0))

        self.is_bad = False
        self.is_master = header.get('ISMASTER', False)
        self.attributes = settings.CALIBRATION_SET_CRITERIA.get(self.obstype, {})

    def _init_instrument_info(self, runtime_context):
        if len(self.header) > 0:
            instrument = dbs.get_instrument(self.header, db_address=runtime_context.db_address)
            site = instrument.site
            camera = instrument.camera
        else:
            instrument, site, camera = None, None, None
        return instrument, site, camera

    def write(self, runtime_context):
        self._save_pipeline_metadata(runtime_context)
        self._update_filename(runtime_context)
        filepath = self._get_filepath(runtime_context)
        self._writeto(filepath, fpack=runtime_context.fpack)
        if self.obstype in settings.CALIBRATION_IMAGE_TYPES:
            dbs.save_calibration_info(filepath, self, db_address=runtime_context.db_address)
        if runtime_context.post_to_archive:
            self._post_to_archive(filepath, runtime_context)

    def _save_pipeline_metadata(self, runtime_context):
        self.datecreated = datetime.datetime.utcnow()
        self.header['DATE'] = (date_utils.date_obs_to_string(self.datecreated), '[UTC] Date this FITS file was written')
        self.header['RLEVEL'] = (runtime_context.rlevel, 'Reduction level')
        self.header['PIPEVER'] = (banzai.__version__, 'Pipeline version')

        if file_utils.instantly_public(self.header['PROPID']):
            self.header['L1PUBDAT'] = (self.header['DATE-OBS'], '[UTC] Date the frame becomes public')
        else:
            # Wait a year
            date_observed = date_utils.parse_date_obs(self.header['DATE-OBS'])
            next_year = date_observed + datetime.timedelta(days=365)
            self.header['L1PUBDAT'] = (date_utils.date_obs_to_string(next_year), '[UTC] Date the frame becomes public')
        logging_tags = {'rlevel': int(self.header['RLEVEL']),
                        'pipeline_version': self.header['PIPEVER'],
                        'l1pubdat': self.header['L1PUBDAT'],}
        logger.info('Adding pipeline metadata to the header', image=self, extra_tags=logging_tags)

    def _update_filename(self, runtime_context):
        self.filename = self.filename.replace('00.fits',
                                              '{:02d}.fits'.format(int(runtime_context.rlevel)))
        if runtime_context.fpack and not self.filename.endswith('.fz'):
            self.filename += '.fz'

    def _get_filepath(self, runtime_context):
        output_directory = file_utils.make_output_directory(runtime_context, self)
        return os.path.join(output_directory, os.path.basename(self.filename))

    def _writeto(self, filepath, fpack=False):
        logger.info('Writing file to {filepath}'.format(filepath=filepath), image=self)
        hdu_list = self._get_hdu_list()
        base_filename = os.path.basename(filepath).split('.fz')[0]
        with tempfile.TemporaryDirectory() as temp_directory:
            hdu_list.writeto(os.path.join(temp_directory, base_filename), overwrite=True, output_verify='fix+warn')
            hdu_list.close()
            if fpack:
                if os.path.exists(filepath):
                    os.remove(filepath)
                command = 'fpack -q 64 {temp_directory}/{basename}'
                os.system(command.format(temp_directory=temp_directory, basename=base_filename))
                base_filename += '.fz'
            shutil.move(os.path.join(temp_directory, base_filename), filepath)

    def _get_hdu_list(self):
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
        return fits.HDUList(hdu_list)

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

    def _post_to_archive(self, filepath, runtime_context):
        logger.info('Posting file to the archive', image=self)
        try:
            file_utils.post_to_archive_queue(filepath, runtime_context.broker_url)
        except Exception:
            logger.error("Could not post to ingester: {error}".format(error=logs.format_exception()), image=self)

    def write_catalog(self, filename, nsources=None):
        if self.data_tables.get('catalog') is None:
            raise exceptions.MissingCatalogException
        else:
            self.data_tables.get('catalog')[:nsources].write(filename, format='fits', overwrite=True)

    def add_history(self, msg):
        self.header.add_history(msg)

    def subtract(self, value):
        self.data -= value

    def update_shape(self, nx, ny):
        self.nx = nx
        self.ny = ny

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
