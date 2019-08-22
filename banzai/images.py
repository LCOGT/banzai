import os
import logging

from banzai import dbs, settings
from banzai.utils import date_utils, file_utils, fits_utils
from banzai import munge

logger = logging.getLogger('banzai')


class Image(object):

    def __init__(self, runtime_context, filename):
        self._hdu_list = fits_utils.init_hdu()

        self.data, self.header, self.bpm, self.extension_headers = fits_utils.open_image(filename)
        self.filename = os.path.basename(filename)

        self.request_number = self.header.get('REQNUM')
        self.instrument = dbs.get_instrument(runtime_context)

        self.epoch = str(self.header.get('DAY-OBS'))
        self.nx = self.header.get('NAXIS1')
        self.ny = self.header.get('NAXIS2')
        self.block_id = self.header.get('BLKUID')
        self.block_start = date_utils.parse_date_obs(self.header.get('BLKSDATE', '1900-01-01T00:00:00.00000'))
        self.molecule_id = self.header.get('MOLUID')

        if len(self.extension_headers) > 0 and 'GAIN' in self.extension_headers[0]:
                self.gain = [h['GAIN'] for h in self.extension_headers]
        else:
            self.gain = eval(str(self.header.get('GAIN')))

        self.ccdsum = self.header.get('CCDSUM')
        self.configuration_mode = fits_utils.get_configuration_mode(self.header)
        self.filter = self.header.get('FILTER')

        self.obstype = self.header.get('OBSTYPE')
        self.exptime = float(self.header.get('EXPTIME', 0.0))
        self.dateobs = date_utils.parse_date_obs(self.header.get('DATE-OBS', '1900-01-01T00:00:00.00000'))
        self.datecreated = date_utils.parse_date_obs(self.header.get('DATE', date_utils.date_obs_to_string(self.dateobs)))
        self.readnoise = float(self.header.get('RDNOISE', 0.0))
        self.ra, self.dec = fits_utils.parse_ra_dec(self.header)
        self.pixel_scale = float(self.header.get('PIXSCALE', 0.0))

        self.is_bad = False
        self.is_master = self.header.get('ISMASTER', False)
        self.attributes = settings.CALIBRATION_SET_CRITERIA.get(self.obstype, {})
        munge.munge(self)

    def __del__(self):
        self._hdu_list.close()
        self._hdu_list._file.close()

    def write(self, runtime_context):
        file_utils.save_pipeline_metadata(self.header, runtime_context.rlevel)
        output_filename = file_utils.make_output_filename(self.filename, runtime_context.fpack, runtime_context.rlevel)
        output_directory = file_utils.make_output_directory(runtime_context.processed_path, self.instrument.site,
                                                            self.instrument.name, self.epoch,
                                                            preview_mode=runtime_context.preview_mode)
        filepath = os.path.join(output_directory, output_filename)
        fits_utils.write_fits_file(filepath, self._hdu_list, runtime_context)
        if self.obstype in settings.CALIBRATION_IMAGE_TYPES:
            dbs.save_calibration_info(filepath, self, db_address=runtime_context.db_address)
        if runtime_context.post_to_archive:
            file_utils.post_to_archive_queue(filepath, runtime_context.broker_url)

    def add_fits_extension(self, extension):
        self._hdu_list.append(extension)

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
