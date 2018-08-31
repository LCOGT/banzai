from __future__ import absolute_import, division, print_function, unicode_literals
import os

import numpy as np
from astropy.io import fits
import tempfile
import shutil

from banzai import dbs
from banzai.utils import date_utils
from banzai.utils import fits_utils
from banzai.utils import image_utils
from banzai import logs
from banzai.munge import munge

logger = logs.get_logger(__name__)


class Image(object):

    def __init__(self, pipeline_context, filename=None, data=None, header=None,
                 extension_headers=None, bpm=None):
        if header is None:
            header = {}

        if extension_headers is None:
            extension_headers = []

        if filename is not None:
            data, header, bpm, extension_headers = fits_utils.open_image(filename)
            if '.fz' == filename[-3:]:
                filename = filename[:-3]
            self.filename = os.path.basename(filename)

        self.data = data
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
        self.catalog = None

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
        if self.catalog is not None:
            table_hdu = fits_utils.table_to_fits(self.catalog)
            table_hdu.name = 'CAT'
            hdu_list.append(table_hdu)
        if self.bpm is not None:
            bpm_hdu = fits.ImageHDU(self.bpm.astype(np.uint8))
            bpm_hdu.name = 'BPM'
            hdu_list.append(bpm_hdu)

        hdu_list = fits.HDUList(hdu_list)
        try:
            hdu_list.verify(option='exception')
        except fits.VerifyError as fits_error:
            logging_tags = logs.image_config_to_tags(self, None)
            logs.add_tag(logging_tags, 'filename', os.path.basename(self.filename))
            logger.warn('Error in FITS Verification. {0}. Attempting fix.'.format(fits_error),
                        extra=logging_tags)
            try:
                hdu_list.verify(option='silentfix+exception')
            except fits.VerifyError as fix_attempt_error:
                logger.error('Could not repair FITS header. {0}'.format(fix_attempt_error),
                             extra=logging_tags)

        with tempfile.TemporaryDirectory() as temp_directory:
            base_filename = os.path.basename(filename)
            hdu_list.writeto(os.path.join(temp_directory, base_filename), overwrite=True,
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

    def update_shape(self, nx, ny):
        self.nx = nx
        self.ny = ny

    def write_catalog(self, filename, nsources=None):
        if self.catalog is None:
            raise image_utils.MissingCatalogException
        else:
            self.catalog[:nsources].write(filename, format='fits', overwrite=True)

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
            logger.error("Cannot get inner section of a 3D image",
                         extra={'tags': {'filename': self.filename}})
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
            logger.error('Error loading image: {error}'.format(error=e), extra={'tags': {'filename': filename}})
            continue
    return images
