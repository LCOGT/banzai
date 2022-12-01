import datetime
import os
from fnmatch import fnmatch
from typing import Optional
from io import BytesIO

import numpy as np
from astropy.io import fits

from astropy.time import Time
from astropy.table import Table
import hashlib

from banzai import dbs
from banzai.data import CCDData, HeaderOnly, DataTable, ArrayData, DataProduct
from banzai.frames import ObservationFrame, CalibrationFrame, logger, FrameFactory
from banzai.utils import date_utils, fits_utils, image_utils, file_utils
from banzai.utils.image_utils import Section

IMAGETYP_TO_OBSTYPE = {
    'zero': 'BIAS',
    'flat': 'SKYFLAT',
    'object': 'EXPOSE',
}


class StewardObservationFrame(ObservationFrame):
    def get_output_directory(self, runtime_context) -> str:
        return os.path.join(runtime_context.processed_path, self.epoch)

    @property
    def obstype(self):
        imagetyp = self.primary_hdu.meta.get('IMAGETYP')
        return IMAGETYP_TO_OBSTYPE.get(imagetyp, imagetyp)

    @property
    def epoch(self):
        dayobs_boundary_utc = 12. - dbs.get_timezone(self.site)  # local noon
        dayobs = self.dateobs
        if self.dateobs.hour < dayobs_boundary_utc:
            dayobs -= datetime.timedelta(days=1.)
        return dayobs.strftime('%Y%m%d')

    @property
    def request_number(self):
        return

    @property
    def block_start(self):
        return

    @property
    def site(self):
        return 'kpno'

    @property
    def camera(self):
        return self.primary_hdu.meta.get('INSTRUME')[:3] + self.primary_hdu.meta.get('CHIP')

    @property
    def filter(self):
        return self.primary_hdu.meta.get('FILTER')

    @property
    def dateobs(self):
        return Time(self.primary_hdu.meta.get('DATE-OBS') + 'T' +
                    self.primary_hdu.meta.get('TIME-OBS'), scale='utc').datetime

    @property
    def datecreated(self):
        return Time(self.primary_hdu.meta.get('DATE'), scale='utc').datetime

    @property
    def configuration_mode(self):
        return 'default'

    @property
    def bias_level(self):
        return self.primary_hdu.meta.get('BIASLVL')

    @bias_level.setter
    def bias_level(self, value):
        self.primary_hdu.meta['BIASLVL'] = value

    @property
    def read_noise(self):
        return self.primary_hdu.meta.get('RDNOISE')

    @read_noise.setter
    def read_noise(self, value):
        self.primary_hdu.meta['RDNOISE'] = value

    @property
    def pixel_scale(self):
        return float(self.primary_hdu.meta.get('PIXSCAL1'))

    @property
    def exptime(self):
        return self.primary_hdu.meta.get('EXPTIME', 0.0)

    @property
    def ccd_temperature(self):
        return int(self.primary_hdu.meta.get('CAMTEMP', 0.0))

    @property
    def requested_ccd_temperature(self):
        return self.primary_hdu.meta.get('CAMTEMP', 0.0)

    @property
    def measured_ccd_temperature(self):
        return self.primary_hdu.meta.get('CAMTEMP', 0.0)

    def save_processing_metadata(self, context):
        datecreated = datetime.datetime.utcnow()
        self.meta['DATE'] = (date_utils.date_obs_to_string(datecreated), '[UTC] Date this FITS file was written')
        self.meta['RLEVEL'] = (context.reduction_level, 'Reduction level')

        self.meta['PIPEVER'] = (context.PIPELINE_VERSION, 'Pipeline version')

    def get_output_filename(self, runtime_context):
        output_filename = self.filename.replace('.fits', '.banzai.fits')
        if runtime_context.fpack and not output_filename.endswith('.fz'):
            output_filename += '.fz'
        if not runtime_context.fpack and output_filename.endswith('.fz'):
            output_filename = output_filename[:-3]
        return output_filename

    def get_output_data_products(self, runtime_context):
        output_filename = self.get_output_filename(runtime_context)
        output_fits = self.to_fits(runtime_context)
        output_product = DataProduct.from_fits(output_fits, output_filename, self.get_output_directory(runtime_context))
        return [output_product]

    def write(self, runtime_context):
        self.save_processing_metadata(runtime_context)
        output_products = self.get_output_data_products(runtime_context)
        for data_product in output_products:
            if runtime_context.post_to_archive:
                archived_image_info = file_utils.post_to_ingester(data_product.file_buffer, self,
                                                                  data_product.filename, meta=data_product.meta)
                self.frame_id = archived_image_info.get('frameid')

            if not runtime_context.no_file_cache:
                os.makedirs(self.get_output_directory(runtime_context), exist_ok=True)
                data_product.file_buffer.seek(0)
                with open(os.path.join(data_product.filepath, data_product.filename), 'wb') as f:
                    f.write(data_product.file_buffer.read())
                logger.info(f'Wrote processed image to {data_product.filename}')

            data_product.file_buffer.seek(0)
            md5 = hashlib.md5(data_product.file_buffer.read()).hexdigest()
            dbs.save_processed_image(data_product.filename, md5, db_address=runtime_context.db_address)
        return output_products


class StewardCalibrationFrame(StewardObservationFrame, CalibrationFrame):
    def __init__(self, hdu_list: list, file_path: str, frame_id: int = None, grouping_criteria: list = None,
                 hdu_order: list = None):
        CalibrationFrame.__init__(self, grouping_criteria=grouping_criteria)
        StewardObservationFrame.__init__(self, hdu_list, file_path, frame_id=frame_id, hdu_order=hdu_order)

    def to_db_record(self, output_product):
        record_attributes = {'type': self.obstype.upper(),
                             'filename': output_product.filename,
                             'filepath': output_product.filepath,
                             'dateobs': self.dateobs,
                             'datecreated': self.datecreated,
                             'instrument_id': self.instrument.id,
                             'is_master': self.is_master,
                             'is_bad': self.is_bad,
                             'frameid': self.frame_id,
                             'attributes': {}}
        for attribute in self.grouping_criteria:
            record_attributes['attributes'][attribute] = str(getattr(self, attribute))
        return dbs.CalibrationImage(**record_attributes)

    @property
    def is_master(self):
        return self.meta.get('ISMASTER', False)

    @is_master.setter
    def is_master(self, value):
        self.meta['ISMASTER'] = value

    def write(self, runtime_context):
        output_products = StewardObservationFrame.write(self, runtime_context)
        CalibrationFrame.write(self, output_products, runtime_context)

    @classmethod
    def init_master_frame(cls, images: list, file_path: str, frame_id: int = None,
                          grouping_criteria: list = None, hdu_order: list = None):
        data_class = type(images[0].primary_hdu)
        hdu_list = [data_class(data=np.zeros(images[0].data.shape, dtype=images[0].data.dtype),
                               meta=cls.init_master_header(images[0].meta, images))]
        frame = cls(hdu_list=hdu_list, file_path=file_path, frame_id=frame_id,
                    grouping_criteria=grouping_criteria, hdu_order=hdu_order)
        frame.is_master = True
        frame.instrument = images[0].instrument
        return frame

    @staticmethod
    def init_master_header(old_header, images):
        header = fits.Header()
        for key in old_header.keys():
            try:
                # Dump empty header keywords and ignore old histories.
                if len(key) > 0 and key != 'HISTORY':
                    for i in range(old_header.count(key)):
                        header[key] = (old_header[(key, i)], old_header.comments[(key, i)])
            except ValueError as e:
                logger.error('Could not add keyword {key}: {error}'.format(key=key, error=e))
                continue
        header = fits_utils.sanitize_header(header)
        observation_dates = [image.dateobs for image in images]
        mean_dateobs = date_utils.mean_date(observation_dates)

        date_obs, time_obs = date_utils.date_obs_to_string(mean_dateobs).split('T')
        header['DATE-OBS'] = (date_obs, '[UTC] Mean observation start time')
        header['TIME-OBS'] = (time_obs, '[UTC] Mean observation start time')
        header['DAY-OBS'] = (max(images, key=lambda x: datetime.datetime.strptime(x.epoch, '%Y%m%d')).epoch, '[UTC] Date at start of local observing night')
        header['ISMASTER'] = (True, 'Is this a master calibration frame')

        header.add_history("Images combined to create master calibration image:")
        for i, image in enumerate(images):
            header[f'IMCOM{i+1:03d}'] = (image.filename, 'Image combined to create master')
        return header


class StewardFrameFactory(FrameFactory):
    @property
    def observation_frame_class(self):
        return StewardObservationFrame

    @property
    def calibration_frame_class(self):
        return StewardCalibrationFrame

    @property
    def data_class(self):
        return CCDData

    @property
    def primary_header_keys_to_propagate(self):
        """
        These are keys that may exist in the PrimaryHDU's header, but
        do not exist in the ImageHDUs.
        """
        return [('RDNOISE', 'RDNOIS{:d}'), ('GAIN', 'GAIN{:d}')]

    @property
    def associated_extensions(self):
        return [{'FITS_NAME': 'BPM', 'NAME': 'mask'}, {'FITS_NAME': 'ERR', 'NAME': 'uncertainty'}]

    def open(self, file_info, runtime_context) -> Optional[ObservationFrame]:
        if file_info.get('RLEVEL') is not None:
            is_raw = file_info.get('RLEVEL', 0) == 0
        else:
            is_raw = False
        fits_hdu_list, filename, frame_id = fits_utils.open_fits_file(file_info, runtime_context, is_raw_frame=is_raw)
        hdu_list = []
        associated_fits_extensions = [associated_extension['FITS_NAME']
                                      for associated_extension in self.associated_extensions]
        # If all of the extensions are arrays we would normally associate with a CCDData object (e.g. BPMs)
        # treat the extensions as normal data
        imagetyp = fits_hdu_list[0].header['IMAGETYP']
        obstype = IMAGETYP_TO_OBSTYPE.get(imagetyp, imagetyp)
        if obstype in associated_fits_extensions or \
                all(hdu.header.get('EXTNAME', '') in associated_fits_extensions
                    for hdu in fits_hdu_list if hdu.data is not None):
            for hdu in fits_hdu_list:
                if hdu.data is None or hdu.data.size == 0:
                    hdu_list.append(HeaderOnly(meta=hdu.header))
                else:
                    hdu_list.append(self.data_class(data=hdu.data, meta=hdu.header, name=hdu.header.get('EXTNAME')))
        else:
            primary_hdu = None
            for i, hdu in enumerate(fits_hdu_list):
                # Move on from any associated arrays like BPM or ERR
                if any(associated_extension['FITS_NAME'] in hdu.header.get('EXTNAME', '')
                       for associated_extension in self.associated_extensions):
                    continue
                # Otherwise parse the fits file into a frame object and the corresponding data objects
                if hdu.data is None or hdu.data.size == 0:
                    hdu_list.append(HeaderOnly(meta=hdu.header))
                    primary_hdu = hdu
                elif isinstance(hdu, fits.BinTableHDU):
                    hdu_list.append(DataTable(data=Table(hdu.data), meta=hdu.header, name=hdu.header.get('EXTNAME')))
                # Assume we are looking at a CCD extension
                else:
                    associated_data = {}
                    condensed_name = hdu.header.get('EXTNAME', '')
                    for extension_name_to_condense in runtime_context.EXTENSION_NAMES_TO_CONDENSE:
                        condensed_name = condensed_name.replace(extension_name_to_condense, '')
                    for associated_extension in self.associated_extensions:
                        associated_fits_extension_name = condensed_name + associated_extension['FITS_NAME']
                        if associated_fits_extension_name in fits_hdu_list:
                            if hdu.header.get('EXTVER') == 0:
                                extension_version = None
                            else:
                                extension_version = hdu.header.get('EXTVER')
                            associated_data[associated_extension['NAME']] = fits_hdu_list[associated_fits_extension_name,
                                                                                          extension_version].data
                        else:
                            associated_data[associated_extension['NAME']] = None

                    if hdu.data.dtype == np.uint16:
                        hdu.data = hdu.data.astype(np.float64)
                    # check if we need to propagate any header keywords from the primary header
                    if primary_hdu is not None:
                        for keyword, primary_keyword in self.primary_header_keys_to_propagate:
                            primary_keyword = primary_keyword.format(i)
                            if primary_keyword in primary_hdu.header and keyword not in hdu.header:
                                hdu.header[keyword] = float(primary_hdu.header[primary_keyword])
                    # For master frames without uncertainties, set to all zeros
                    if hdu.header.get('ISMASTER', False) and associated_data['uncertainty'] is None:
                        associated_data['uncertainty'] = np.zeros(hdu.data.shape, dtype=hdu.data.dtype)
                    hdu_list.append(self.data_class(data=hdu.data, meta=hdu.header, name=hdu.header.get('EXTNAME'),
                                                    **associated_data))

        # Either use the calibration frame type or normal frame type depending on the IMAGETYP keyword
        imagetyp = hdu_list[0].meta.get('IMAGETYP')
        obstype = IMAGETYP_TO_OBSTYPE.get(imagetyp, imagetyp)
        hdu_order = runtime_context.REDUCED_DATA_EXTENSION_ORDERING.get(obstype)

        if obstype in runtime_context.CALIBRATION_IMAGE_TYPES:
            grouping = runtime_context.CALIBRATION_SET_CRITERIA.get(obstype, [])
            image = self.calibration_frame_class(hdu_list, filename, frame_id=frame_id, grouping_criteria=grouping,
                                                 hdu_order=hdu_order)
        else:
            image = self.observation_frame_class(hdu_list, filename, frame_id=frame_id, hdu_order=hdu_order)
        image.instrument = self.get_instrument_from_header(image.primary_hdu.meta, runtime_context.db_address)
        if image.instrument is None:
            return None

        self._init_saturate(image)

        # If the frame cannot be processed for some reason return None instead of the new image object
        if image_utils.image_can_be_processed(image, runtime_context):
            return image
        else:
            return None

    @staticmethod
    def get_instrument_from_header(header, db_address=None):
        site = 'kpno'
        camera = header.get('INSTRUME')[:3] + header.get('CHIP')
        instrument = dbs.query_for_instrument(site, camera, db_address=db_address)
        if instrument is None:
            msg = 'Instrument is not in the database, Please add it before reducing this data.'
            tags = {'site': site, 'camera': camera}
            logger.debug(msg, extra_tags=tags)
        return instrument

    @staticmethod
    def _init_saturate(image):
        defaults = {'90prime': 65535.}  # ADU (before gain)

        default_unbinned_saturation = None
        for instrument_type in defaults:
            if instrument_type in image.instrument.type.lower():
                default_unbinned_saturation = defaults[instrument_type]
                break

        for hdu in image.ccd_hdus:
            # Pull from the primary extension by default
            if hdu.meta.get('SATURATE', 0.0) == 0.0:
                hdu.meta['SATURATE'] = image.meta.get('SATURATE', 0.0)
                hdu.meta['MAXLIN'] = image.meta.get('MAXLIN', 0.0)
            # If still nothing, use hard coded defaults
            if hdu.meta.get('SATURATE', 0.0) == 0.0 and default_unbinned_saturation is not None:
                n_binned_pixels = hdu.binning[0] * hdu.binning[1]
                default = default_unbinned_saturation * n_binned_pixels
                image.meta['SATURATE'] = (default, '[ADU] Saturation level used')
                hdu.meta['SATURATE'] = (default, '[ADU] Saturation level used')
                hdu.meta['MAXLIN'] = (default, '[ADU] Non-linearity level')

        if 0.0 in [hdu.meta.get('SATURATE', 0.0) for hdu in image.ccd_hdus]:
            logger.error('The SATURATE keyword was not valid and there are no defaults in banzai for this camera.',
                         image=image)


def telescope_to_filename(image):
    telescope = image.meta.get('TELESCOP', '')
    if 'bok' in telescope.lower():
        telescope = '2m3bok'
    return telescope
