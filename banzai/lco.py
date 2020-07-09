import datetime
import os
from fnmatch import fnmatch
from typing import Optional

import numpy as np
from astropy.io import fits

from astropy.time import Time
from astropy.table import Table

from banzai import dbs
from banzai.data import CCDData, HeaderOnly, DataTable, ArrayData
from banzai.frames import ObservationFrame, CalibrationFrame, logger, FrameFactory
from banzai.utils import date_utils, fits_utils, image_utils
from banzai.utils.image_utils import Section


class LCOObservationFrame(ObservationFrame):
    # TODO: Set all status check keywords to bad if they are not already set
    # TODO: Add gain validation
    def get_output_filename(self, runtime_context):
        # TODO add a mode for AWS filenames
        output_directory = os.path.join(runtime_context.processed_path, self.instrument.site,
                                        self.instrument.camera, self.epoch, 'processed')
        output_filename = self._file_path.replace('00.fits', '{:02d}.fits'.format(int(runtime_context.reduction_level)))
        output_filename = os.path.join(output_directory, os.path.basename(output_filename))
        if runtime_context.fpack and not output_filename.endswith('.fz'):
            output_filename += '.fz'
        if not runtime_context.fpack and output_filename.endswith('.fz'):
            output_filename = output_filename[:-3]
        return output_filename

    @property
    def n_amps(self):
        return len(self.ccd_hdus)

    @property
    def obstype(self):
        return self.primary_hdu.meta.get('OBSTYPE')

    @property
    def epoch(self):
        return self.primary_hdu.meta.get('DAY-OBS')

    @property
    def request_number(self):
        return self.primary_hdu.meta.get('REQNUM')

    @property
    def block_start(self):
        return Time(self.primary_hdu.meta.get('BLKSDATE'), scale='utc').datetime

    @property
    def site(self):
        return self.primary_hdu.meta.get('SITEID')

    @property
    def camera(self):
        return self.primary_hdu.meta.get('INSTRUME')

    @property
    def filter(self):
        return self.primary_hdu.meta.get('FILTER')

    @property
    def dateobs(self):
        return Time(self.primary_hdu.meta.get('DATE-OBS'), scale='utc').datetime

    @property
    def datecreated(self):
        return Time(self.primary_hdu.meta.get('DATE'), scale='utc').datetime

    @property
    def catalog(self):
        return next((hdu.data for hdu in self._hdus if hdu.name == 'CAT'), None)

    @catalog.setter
    def catalog(self, value: DataTable):
        self._hdus.append(value)

    @property
    def configuration_mode(self):
        mode = self.meta.get('CONFMODE', 'default')
        if str(mode).lower() in ['n/a', '0', 'normal']:
            mode = 'default'
        return mode

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
        return self.primary_hdu.meta.get('PIXSCALE')

    @property
    def exptime(self):
        return self.primary_hdu.meta.get('EXPTIME', 0.0)

    def save_processing_metadata(self, context):
        datecreated = datetime.datetime.utcnow()
        self.meta['DATE'] = (date_utils.date_obs_to_string(datecreated), '[UTC] Date this FITS file was written')
        self.meta['RLEVEL'] = (context.reduction_level, 'Reduction level')

        self.meta['PIPEVER'] = (context.PIPELINE_VERSION, 'Pipeline version')

        if any(fnmatch(self.meta['PROPID'].lower(), public_proposal) for public_proposal in context.PUBLIC_PROPOSALS):
            self.meta['L1PUBDAT'] = (self.meta['DATE-OBS'], '[UTC] Date the frame becomes public')
        else:
            # Wait to make public
            date_observed = date_utils.parse_date_obs(self.meta['DATE-OBS'])
            next_year = date_observed + datetime.timedelta(days=context.DATA_RELEASE_DELAY)
            self.meta['L1PUBDAT'] = (date_utils.date_obs_to_string(next_year), '[UTC] Date the frame becomes public')


class LCOCalibrationFrame(LCOObservationFrame, CalibrationFrame):
    def __init__(self, hdu_list: list, file_path: str, frame_id: int = None, grouping_criteria: list = None,
                 hdu_order: list = None):
        CalibrationFrame.__init__(self, grouping_criteria=grouping_criteria)
        LCOObservationFrame.__init__(self, hdu_list, file_path, frame_id=frame_id, hdu_order=hdu_order)

    @property
    def is_master(self):
        return self.meta.get('ISMASTER', False)

    @is_master.setter
    def is_master(self, value):
        self.meta['ISMASTER'] = value

    def write(self, runtime_context):
        LCOObservationFrame.write(self, runtime_context)
        CalibrationFrame.write(self, runtime_context)

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

        header['DATE-OBS'] = (date_utils.date_obs_to_string(mean_dateobs), '[UTC] Mean observation start time')
        header['DAY-OBS'] = (max(images, key=lambda x: datetime.datetime.strptime(x.epoch, '%Y%m%d')).epoch, '[UTC] Date at start of local observing night')
        header['ISMASTER'] = (True, 'Is this a master calibration frame')

        header.add_history("Images combined to create master calibration image:")
        for i, image in enumerate(images):
            header[f'IMCOM{i+1:03d}'] = (image.filename, 'Image combined to create master')
        return header


class MissingCrosstalkCoefficients(Exception):
    pass


class MissingSaturate(Exception):
    pass


"""These matrices should have the following structure:
coeffs = [[Q11, Q12, Q13, Q14],
          [Q21, Q22, Q23, Q24],
          [Q31, Q32, Q33, Q34],
          [Q41, Q42, Q43, Q44]]

The corrected data, D, from quadrant i is
D1 = D1 - Q21 D2 - Q31 D3 - Q41 D4
D2 = D2 - Q12 D1 - Q32 D3 - Q42 D4
D3 = D3 - Q13 D1 - Q23 D2 - Q43 D4
D4 = D4 - Q14 D1 - Q24 D2 - Q34 D3
"""
DEFAULT_CROSSTALK_COEFFICIENTS = {'fl01': np.array([[0.00000, 0.00074, 0.00081, 0.00115],
                                                    [0.00070, 0.00000, 0.00118, 0.00085],
                                                    [0.00076, 0.00115, 0.00000, 0.00088],
                                                    [0.00107, 0.00075, 0.00080, 0.00000]]),
                                  'fl02': np.array([[0.00000, 0.00084, 0.00088, 0.00125],
                                                    [0.00083, 0.00000, 0.00124, 0.00096],
                                                    [0.00086, 0.00121, 0.00000, 0.00098],
                                                    [0.00116, 0.00085, 0.00092, 0.00000]]),
                                  'fl03': np.array([[0.00000, 0.00076, 0.00079, 0.00115],
                                                    [0.00073, 0.00000, 0.00117, 0.00084],
                                                    [0.00074, 0.00113, 0.00000, 0.00084],
                                                    [0.00105, 0.00075, 0.00080, 0.00000]]),
                                  'fl04': np.array([[0.00000, 0.00088, 0.00096, 0.00131],
                                                    [0.00087, 0.00000, 0.00132, 0.00099],
                                                    [0.00087, 0.00127, 0.00000, 0.00103],
                                                    [0.00123, 0.00089, 0.00094, 0.00000]]),
                                  'fl05': np.array([[0.00000, 0.00084, 0.00090, 0.00126],
                                                    [0.00089, 0.00000, 0.00133, 0.00095],
                                                    [0.00097, 0.00155, 0.00000, 0.00108],
                                                    [0.00134, 0.00096, 0.00095, 0.00000]]),
                                  'fl06': np.array([[0.00000, 0.00076, 0.00068, 0.00129],
                                                    [0.00082, 0.00000, 0.00141, 0.00090],
                                                    [0.00095, 0.00124, 0.00000, 0.00107],
                                                    [0.00110, 0.00076, 0.00106, 0.00000]]),
                                  'fl07': np.array([[0.00000, 0.00075, 0.00077, 0.00113],
                                                    [0.00071, 0.00000, 0.00113, 0.00082],
                                                    [0.00070, 0.00108, 0.00000, 0.00086],
                                                    [0.00095, 0.00067, 0.00077, 0.00000]]),
                                  'fl08': np.array([[0.00000, 0.00057, 0.00078, 0.00130],
                                                    [0.00112, 0.00000, 0.00163, 0.00123],
                                                    [0.00104, 0.00113, 0.00000, 0.00113],
                                                    [0.00108, 0.00048, 0.00065, 0.00000]]),
                                  'fl10': np.array([[0.00000, 0.00000, 0.00000, 0.00000],
                                                    [0.00000, 0.00000, 0.00000, 0.00000],
                                                    [0.00000, 0.00000, 0.00000, 0.00000],
                                                    [0.00000, 0.00000, 0.00000, 0.00000]]),
                                  'fl11': np.array([[0.00000, 0.00075, 0.00078, 0.00113],
                                                    [0.00065, 0.00000, 0.00114, 0.00096],
                                                    [0.00070, 0.00101, 0.00000, 0.00086],
                                                    [0.00098, 0.00073, 0.00082, 0.00000]]),
                                  'fl12': np.array([[0.00000, 0.00083, 0.00089, 0.00127],
                                                    [0.00079, 0.00000, 0.00117, 0.00091],
                                                    [0.00081, 0.00113, 0.00000, 0.00094],
                                                    [0.00105, 0.00081, 0.00087, 0.00000]]),
                                  'fl14': np.array([[0.00000, 0.00084, 0.00086, 0.00121],
                                                    [0.00094, 0.00000, 0.00134, 0.00103],
                                                    [0.00094, 0.00129, 0.00000, 0.00105],
                                                    [0.00097, 0.00092, 0.00099, 0.00000]]),
                                  'fl15': np.array([[0.00000, 0.00071, 0.00083, 0.00110],
                                                    [0.00069, 0.00000, 0.00107, 0.00081],
                                                    [0.00071, 0.00098, 0.00000, 0.00083],
                                                    [0.00091, 0.00071, 0.00078, 0.00000]]),
                                  'fl16': np.array([[0.00000, 0.00080, 0.00084, 0.00125],
                                                    [0.00071, 0.00000, 0.00122, 0.00088],
                                                    [0.00071, 0.00121, 0.00000, 0.00090],
                                                    [0.00116, 0.00084, 0.00089, 0.00000]]),
                                  # Archon-controlled imagers as of fall 2018
                                  'fa01': np.array([[0.00000, 0.00074, 0.00081, 0.00115],
                                                    [0.00070, 0.00000, 0.00118, 0.00085],
                                                    [0.00076, 0.00115, 0.00000, 0.00088],
                                                    [0.00107, 0.00075, 0.00080, 0.00000]]),
                                  'fa02': np.array([[0.00000, 0.00084, 0.00088, 0.00125],
                                                    [0.00083, 0.00000, 0.00124, 0.00096],
                                                    [0.00086, 0.00121, 0.00000, 0.00098],
                                                    [0.00116, 0.00085, 0.00092, 0.00000]]),
                                  'fa03': np.array([[0.00000, 0.00076, 0.00079, 0.00115],
                                                    [0.00073, 0.00000, 0.00117, 0.00084],
                                                    [0.00074, 0.00113, 0.00000, 0.00084],
                                                    [0.00105, 0.00075, 0.00080, 0.00000]]),
                                  'fa04': np.array([[0.00000, 0.00088, 0.00096, 0.00131],
                                                    [0.00087, 0.00000, 0.00132, 0.00099],
                                                    [0.00087, 0.00127, 0.00000, 0.00103],
                                                    [0.00123, 0.00089, 0.00094, 0.00000]]),
                                  'fa05': np.array([[0.00000, 0.00084, 0.00090, 0.00126],
                                                    [0.00089, 0.00000, 0.00133, 0.00095],
                                                    [0.00097, 0.00155, 0.00000, 0.00108],
                                                    [0.00134, 0.00096, 0.00095, 0.00000]]),
                                  'fa06': np.array([[0.00000, 0.00076, 0.00068, 0.00129],
                                                    [0.00082, 0.00000, 0.00141, 0.00090],
                                                    [0.00095, 0.00124, 0.00000, 0.00107],
                                                    [0.00110, 0.00076, 0.00106, 0.00000]]),
                                  'fa07': np.array([[0.00000, 0.00075, 0.00077, 0.00113],
                                                    [0.00071, 0.00000, 0.00113, 0.00082],
                                                    [0.00070, 0.00108, 0.00000, 0.00086],
                                                    [0.00095, 0.00067, 0.00077, 0.00000]]),
                                  'fa08': np.array([[0.00000, 0.00057, 0.00078, 0.00130],
                                                    [0.00112, 0.00000, 0.00163, 0.00123],
                                                    [0.00104, 0.00113, 0.00000, 0.00113],
                                                    [0.00108, 0.00048, 0.00065, 0.00000]]),
                                  'fa10': np.array([[0.00000, 0.00000, 0.00000, 0.00000],
                                                    [0.00000, 0.00000, 0.00000, 0.00000],
                                                    [0.00000, 0.00000, 0.00000, 0.00000],
                                                    [0.00000, 0.00000, 0.00000, 0.00000]]),
                                  'fa11': np.array([[0.00000, 0.00075, 0.00078, 0.00113],
                                                    [0.00065, 0.00000, 0.00114, 0.00096],
                                                    [0.00070, 0.00101, 0.00000, 0.00086],
                                                    [0.00098, 0.00073, 0.00082, 0.00000]]),
                                  'fa12': np.array([[0.00000, 0.00083, 0.00089, 0.00127],
                                                    [0.00079, 0.00000, 0.00117, 0.00091],
                                                    [0.00081, 0.00113, 0.00000, 0.00094],
                                                    [0.00105, 0.00081, 0.00087, 0.00000]]),
                                  'fa14': np.array([[0.00000, 0.00084, 0.00086, 0.00121],
                                                    [0.00094, 0.00000, 0.00134, 0.00103],
                                                    [0.00094, 0.00129, 0.00000, 0.00105],
                                                    [0.00097, 0.00092, 0.00099, 0.00000]]),
                                  'fa15': np.array([[0.00000, 0.00071, 0.00083, 0.00110],
                                                    [0.00069, 0.00000, 0.00107, 0.00081],
                                                    [0.00071, 0.00098, 0.00000, 0.00083],
                                                    [0.00091, 0.00071, 0.00078, 0.00000]]),
                                  'fa16': np.array([[0.00000, 0.00080, 0.00084, 0.00125],
                                                    [0.00071, 0.00000, 0.00122, 0.00088],
                                                    [0.00071, 0.00121, 0.00000, 0.00090],
                                                    [0.00116, 0.00084, 0.00089, 0.00000]]),
                                  'fa19': np.array([[0.00000, 0.00080, 0.00084, 0.00125],
                                                    [0.00071, 0.00000, 0.00122, 0.00088],
                                                    [0.00071, 0.00121, 0.00000, 0.00090],
                                                    [0.00116, 0.00084, 0.00089, 0.00000]])
                                  }


class LCOFrameFactory(FrameFactory):
    @property
    def observation_frame_class(self):
        return LCOObservationFrame

    @property
    def calibration_frame_class(self):
        return LCOCalibrationFrame

    @property
    def data_class(self):
        return CCDData

    @property
    def primary_header_keys_to_propagate(self):
        """
        These are keys that may exist in the PrimaryHDU's header, but
        do not exist in the ImageHDUs.
        """
        return ['RDNOISE']

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
        if fits_hdu_list[0].header['OBSTYPE'] in associated_fits_extensions or \
                all(hdu.header.get('EXTNAME', '') in associated_fits_extensions
                    for hdu in fits_hdu_list if hdu.data is not None):
            for hdu in fits_hdu_list:
                if hdu.data is None:
                    hdu_list.append(HeaderOnly(meta=hdu.header))
                else:
                    hdu_list.append(self.data_class(data=hdu.data, meta=hdu.header, name=hdu.header.get('EXTNAME')))
        else:
            primary_hdu = None
            for hdu in fits_hdu_list:
                # Move on from any associated arrays like BPM or ERR
                if any(associated_extension['FITS_NAME'] in hdu.header.get('EXTNAME', '')
                       for associated_extension in self.associated_extensions):
                    continue
                # Otherwise parse the fits file into a frame object and the corresponding data objects
                if hdu.data is None:
                    hdu_list.append(HeaderOnly(meta=hdu.header))
                    primary_hdu = hdu
                elif isinstance(hdu, fits.BinTableHDU):
                    hdu_list.append(DataTable(data=Table(hdu.data), name=hdu.header.get('EXTNAME'), meta=hdu.header))
                # Check if we are looking at a CCD extension
                elif 'GAIN' in hdu.header:
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
                    if len(hdu.data.shape) > 2:
                        hdu_list += self._munge_data_cube(hdu)
                    # update datasec/trimsec for fs01
                    if hdu.header.get('INSTRUME') == 'fs01':
                        self._update_fs01_sections(hdu)
                    if hdu.data.dtype == np.uint16:
                        hdu.data = hdu.data.astype(np.float64)
                    # check if we need to propagate any header keywords from the primary header
                    if primary_hdu is not None:
                        for keyword in self.primary_header_keys_to_propagate:
                            if keyword in primary_hdu.header and keyword not in hdu.header:
                                hdu.header[keyword] = primary_hdu.header[keyword]
                    # For master frames without uncertainties, set to all zeros
                    if hdu.header.get('ISMASTER', False) and associated_data['uncertainty'] is None:
                        associated_data['uncertainty'] = np.zeros(hdu.data.shape, dtype=hdu.data.dtype)
                    hdu_list.append(self.data_class(data=hdu.data, meta=hdu.header, name=hdu.header.get('EXTNAME'),
                                                    **associated_data))
                else:
                    hdu_list.append(ArrayData(data=hdu.data, meta=hdu.header, name=hdu.header.get('EXTNAME')))

        # Either use the calibration frame type or normal frame type depending on the OBSTYPE keyword
        hdu_order = runtime_context.REDUCED_DATA_EXTENSION_ORDERING.get(hdu_list[0].meta.get('OBSTYPE'))

        if hdu_list[0].meta.get('OBSTYPE') in runtime_context.CALIBRATION_IMAGE_TYPES:
            grouping = runtime_context.CALIBRATION_SET_CRITERIA.get(hdu_list[0].meta.get('OBSTYPE'), [])
            image = self.calibration_frame_class(hdu_list, filename, frame_id=frame_id, grouping_criteria=grouping,
                                                 hdu_order=hdu_order)
        else:
            image = self.observation_frame_class(hdu_list, filename, frame_id=frame_id, hdu_order=hdu_order)
        image.instrument = self.get_instrument_from_header(image.primary_hdu.meta, runtime_context.db_address)
        if image.instrument is None:
            return None

        # Do some munging specific to LCO data when our headers were not complete
        self._init_detector_sections(image)
        self._init_saturate(image)
        try:
            self._init_crosstalk(image)
        except MissingCrosstalkCoefficients:
            logger.error('Crosstalk coefficients are missing from both the header and the defaults. Stopping reduction',
                         image=image)
            return None
        # If the frame cannot be processed for some reason return None instead of the new image object
        if image_utils.image_can_be_processed(image, runtime_context):
            return image
        else:
            return None

    @staticmethod
    def get_instrument_from_header(header, db_address):
        site = header.get('SITEID')
        camera = header.get('INSTRUME')
        instrument = dbs.query_for_instrument(db_address, site, camera)
        name = camera
        if instrument is None:
            # if instrument is missing, assume it is an NRES frame and check for the instrument again.
            name = header.get('TELESCOP')
            instrument = dbs.query_for_instrument(db_address, site, camera, name=name)
        if instrument is None:
            msg = 'Instrument is not in the database, Please add it before reducing this data.'
            tags = {'site': site, 'camera': camera, 'telescop': name}
            logger.debug(msg, extra_tags=tags)
        return instrument

    @staticmethod
    def _init_saturate(image):
        # Spectral values were given by Joe Tufts on 2016-06-07
        # Sbig 1m's from ORAC
        # Sbigs 0.4m and 0.8m values measured by Daniel Harbeck
        defaults = {'1m0-scicam-sinistro': 47500.0 * 2.0, '1m0-scicam-sbig': 46000.0 / 4 * 1.4,
                    '0m8': 64000.0 / 4 * 0.851, '0m4': 64000.0 / 4, 'spectral': 125000.0}

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
                default = default_unbinned_saturation * n_binned_pixels / hdu.gain
                hdu.meta['SATURATE'] = (default, '[ADU] Saturation level used')
                hdu.meta['MAXLIN'] = (default, '[ADU] Non-linearity level')

        if 0.0 in [hdu.meta.get('SATURATE', 0.0) for hdu in image.ccd_hdus]:
            logger.error('The SATURATE keyword was not valid and there are no defaults in banzai for this camera.',
                         image=image)
            raise MissingSaturate

    @staticmethod
    def _update_fs01_sections(hdu):
        """
        Manually update data and trim sections for fs01 spectral camera
        :param hdu: Astropy ImageHDU
        """
        old_section_keywords = {'TRIMSEC': '[11:2055,19:2031]',
                                'DATASEC': '[1:2048,1:2048]'}
        new_section_keywords = {'TRIMSEC': '[2:2046,4:2016]',
                                'DATASEC': '[10:2056,16:2032]'}

        for key in old_section_keywords:
            if hdu.header[key] == old_section_keywords[key]:
                hdu.header[key] = new_section_keywords[key]

    @staticmethod
    def _init_detector_sections(image):
        for hdu in image.ccd_hdus:
            if hdu.meta.get('DETSEC', 'UNKNOWN') in ['UNKNOWN', 'N/A']:
                # DETSEC missing?
                binning = hdu.meta.get('CCDSUM', image.primary_hdu.meta.get('CCDSUM', '1 1'))
                if hdu.data_section is not None:
                    # use binning from FITS header, bin_x is index 0, bin_y is index 2.
                    detector_section = Section(1, max(hdu.data_section.x_start, hdu.data_section.x_stop) * int(binning[0]),
                                               1, max(hdu.data_section.y_start, hdu.data_section.y_stop) * int(binning[2]))
                    hdu.detector_section = detector_section
                else:
                    logger.warning("Data and detector sections are both undefined for image.", image=image)

    @staticmethod
    def _init_crosstalk(image):
        n_amps = image.n_amps
        coefficients = DEFAULT_CROSSTALK_COEFFICIENTS.get(image.camera)

        for i in range(n_amps):
            for j in range(n_amps):
                if i != j:
                    crosstalk_comment = '[Crosstalk coefficient] Signal from Q{i} onto Q{j}'.format(i=i+1, j=j+1)
                    keyword = 'CRSTLK{0}{1}'.format(i + 1, j + 1)
                    # Don't override existing header keywords.
                    if image.primary_hdu.meta.get(keyword) is None and coefficients is not None:
                        image.primary_hdu.meta[keyword] = coefficients[i, j], crosstalk_comment
        crosstalk_values = []
        for i in range(n_amps):
            for j in range(n_amps):
                if i != j:
                    crosstalk_values.append(image.meta.get('CRSTLK{0}{1}'.format(i+1, j+1), None))
        if None in crosstalk_values:
            raise MissingCrosstalkCoefficients

    @staticmethod
    def _munge_data_cube(hdu):
        """
        Munge the old sinistro data cube data into our new format

        :param hdu: Fits.ImageHDU
        :return: List CCDData objects
        """
        # The first extension gets to be a header only object
        hdu_list = [HeaderOnly(meta=hdu.header)]

        # We need to properly set the datasec and detsec keywords in case we didn't read out the
        # middle row (the "Missing Row Problem").
        sinistro_datasecs = {'missing': ['[1:2048,1:2048]', '[1:2048,1:2048]',
                                         '[1:2048,2:2048]', '[1:2048,2:2048]'],
                             'full': ['[1:2048,1:2048]', '[1:2048,1:2048]',
                                      '[1:2048,2:2049]', '[1:2048,2:2049]']}
        sinistro_detsecs = {'missing': ['[1:2048,1:2048]', '[4096:2049,1:2048]',
                                        '[4096:2049,4096:2050]', '[1:2048,4096:2050]'],
                            'full': ['[1:2048,1:2048]', '[4096:2049,1:2048]',
                                     '[4096:2049,4096:2049]', '[1:2048,4096:2049]']}
        for i in range(hdu.data.shape[0]):
            gain = eval(hdu.header['GAIN'])[i]
            if hdu.data.shape[1] > 2048:
                mode = 'full'
            else:
                mode = 'missing'
            datasec = sinistro_datasecs[mode][i]
            detsec = sinistro_detsecs[mode][i]
            header = {'BIASSEC': ('[2055:2080,1:2048]', '[binned pixel] Overscan Region'),
                      'GAIN': (gain, hdu.header.comments['GAIN']),
                      'DATASEC': (datasec, '[binned pixel] Data section'),
                      'DETSEC': (detsec, '[unbinned pixel] Detector section'),
                      'CCDSUM': (hdu.header['CCDSUM'], hdu.header.comments['CCDSUM'])}
            hdu_list.append(CCDData(data=hdu.data[i], meta=fits.Header(header)))
        # We have to split the gain keyword for each extension
        return hdu_list
