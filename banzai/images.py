#import os
import logging

from banzai import dbs
#from banzai.utils import date_utils, file_utils, fits_utils
from banzai.utils import fits_utils
#from banzai import munge, settings
import numpy as np
from astropy.io import fits
from astropy.table import Table
from astropy.time import Time
from typing import Union, Type
import tempfile
import abc
import os

logger = logging.getLogger('banzai')


class Data(metaclass=abc.ABCMeta):
    _file_handles = []

    def __init__(self, data: Union[np.array, Table], meta: Union[dict, fits.Header],
                 mask: np.array = None, name: str = ''):
        self.data = self._init_array(data)
        self.meta = meta
        self._validate_mask(mask)
        self.mask = self._init_array(mask, dtype=np.uint8)
        self.name = name

    def _validate_mask(self, mask):
        if mask is not None:
            if mask.shape != self.data.shape:
                raise ValueError('Mask must have the same dimensions as the data')

    def _init_array(self, array: np.array = None, dtype: Type = None):
        file_handle = tempfile.NamedTemporaryFile('w+b')
        if array is None:
            shape = self.data.shape
            if dtype is None:
                dtype = self.data.dtype
            array = np.zeros(shape, dtype=dtype)
        if array.size > 0:
            memory_mapped_array = np.memmap(file_handle, shape=array.shape, dtype=array.dtype, mode='readwrite')
            memory_mapped_array.ravel()[:] = array.ravel()[:]
            self._file_handles.append(file_handle)
        else:
            memory_mapped_array = array
        return memory_mapped_array

    def add_mask(self, mask: np.array):
        self._validate_mask(mask)
        self.mask = self._init_array(mask)

    def __del__(self):
        for handle in self._file_handles:
            handle.close()
        del self.data
        del self.mask

    @property
    def extension_name(self):
        return self.meta.get('EXTNAME').replace('SCI', '')

    @extension_name.setter
    def extension_name(self, value):
        self.meta['EXTNAME'] = value

    @classmethod
    def from_fits(cls, hdu: Union[fits.ImageHDU, fits.TableHDU, fits.BinTableHDU]):
        return cls(hdu.data, hdu.header, name=hdu.header.get('EXTNAME'))

    @abc.abstractmethod
    def to_fits(self) -> Union[fits.HDUList, list]:
        pass


class HeaderOnly(Data):
    def __init__(self, meta: Union[dict, fits.Header]):
        super().__init__(data=np.zeros(0), meta=meta)

    def to_fits(self):
        return fits.HDUList([fits.ImageHDU(data=None, header=self.meta)])


class CCDData(Data):
    STATUS_KEYWORDS = {'overscan': {'L1STATOV': ('1', 'Status flag for overscan correction'),
                                    'OVERSCAN': ('value', 'Overscan value that was subtracted')},
                       'bias_level': {'BIASLVL': ('value', 'Bias level that was removed after overscan')}
                       }

    def __init__(self, data: Union[np.array, Table], meta: Union[dict, fits.Header],
                 mask: np.array = None, name: str = '', uncertainty: np.array = None):
        super().__init__(data=data, meta=meta, mask=mask, name=name)
        self.read_noise = meta.get('RDNOISE', 0.0)
        if uncertainty is None:
            uncertainty = self.read_noise * np.ones(data.shape, dtype=data.dtype)
        self.uncertainty = self._init_array(uncertainty)

    def __getitem__(self, section):
        """
        Return a new CCDData object with the given section of data
        :param section:
        :return:
        """
        pass

    def __imul__(self, value):
        self.data *= value
        self.meta['SATURATE'] *= value
        self.meta['GAIN'] *= value
        self.meta['MAXLIN'] *= value

    def to_fits(self):
        data_hdu = fits.ImageHDU(data=self.data, header=fits.Header(self.meta))
        bpm_extname = self.extension_name + 'BPM'
        mask_hdu = fits.ImageHDU(data=self.mask, header=fits.Header({'EXTNAME': bpm_extname}))
        uncertainty_extname = self.extension_name + 'ERR'
        uncertainty_hdu = fits.ImageHDU(data=self.uncertainty, header=fits.Header({'EXTNAME': uncertainty_extname}))
        return fits.HDUList([data_hdu, mask_hdu, uncertainty_hdu])

    def __del__(self):
        super().__del__()
        del self.uncertainty

    def subtract(self, value, kind=None):
        if isinstance(value, CCDData):
            self.data -= value.data
            self.uncertainty = np.sqrt(value.uncertainty * value.uncertainty + self.uncertainty * self.uncertainty)
            self.mask |= value.mask
        else:
            self.data -= value
        if kind is not None:
            for keyword, (status_value, comment) in self.STATUS_KEYWORDS[kind].items():
                self.meta[keyword] = eval(status_value), comment

    def get_overscan_region(self):
        return Section.parse_region_keyword(self.meta.get('BIASSEC', 'N/A'))

    def trim(self, trim_section=None):
        if trim_section is None:
            trim_section = Section.parse_region_keyword(self.meta.get('TRIMSEC', 'N/A'))
        trimmed_image = CCDData(self.data[trim_section.to_slice()], self.meta,
                                self.mask[trim_section.to_slice()], self.name,
                                uncertainty=self.uncertainty[trim_section.to_slice()])
        # TODO: update all section keywords, DATASEC, DETSEC, CCDSEC
        return trimmed_image

    @property
    def dtype(self):
        return self.data.dtype

    @property
    def shape(self):
        return self.data.shape

    @property
    def gain(self):
        return self.meta['GAIN']

    @property
    def binning(self):
        return [int(b) for b in self.meta.get('CCDSUM', '1 1').split(' ')]

    @binning.setter
    def binning(self, value):
        x_binning, y_binning = value
        self.meta['CCDSUM'] = f'{x_binning} {y_binning}'

    @property
    def detector_section(self):
        return Section.parse_region_keyword(self.meta.get('DETSEC'))

    @detector_section.setter
    def detector_section(self, section):
        self.meta['DETSEC'] = section.to_region_keyword()

    @property
    def _data_section(self):
        return Section.parse_region_keyword(self.meta.get('DATASEC'))

    @_data_section.setter
    def _data_section(self, section):
        self.meta['DATASEC'] = section.to_region_keyword()

    def rebin(self, binning):
        # TODO: Implement me
        return self

    def get_overlap(self, detector_section):
        return self.detector_section.overlap(detector_section)

    def get_data_section(self, region):
        """Given a detector region, figure out the corresponding data region
        Note the + and - 1 factors cancel
        Really this is just doing the same thing as a CD matrix calculation
        r_data - data_0 = M (r_det - det_0) where M is a transformation matrix and everything else is a 2d vector
        M is either the 1/binning * identity or the 1/binning * negative Identity depending on if datasec and detsec
        are in the same ordering (both increasing or both decreasing gives positive)
        Note if you want to add a rotation, M can contain that as well.
        """
        def get_data_section_oned(axis):
            binning_indices = {'x': 0, 'y': 1}
            detector_section = self.detector_section
            data_section = self._data_section
            sign = np.sign(getattr(detector_section, f'{axis}_stop') - getattr(detector_section, f'{axis}_start'))
            sign *= np.sign(getattr(data_section, f'{axis}_stop') - getattr(data_section, f'{axis}_start'))
            start = sign * (getattr(region, f'{axis}_start') - getattr(detector_section, f'{axis}_start'))
            start //= self.binning[binning_indices[axis]]
            start += getattr(data_section, f'{axis}_start')
            stop = sign * (getattr(region, f'{axis}_stop') - getattr(self.detector_section, f'{axis}_start'))
            stop //= self.binning[binning_indices[axis]]
            stop += getattr(data_section, f'{axis}_start')
            return start, stop

        x_start, x_stop = get_data_section_oned('x')
        y_start, y_stop = get_data_section_oned('y')

        return Section(x_start, x_stop, y_start, y_stop)

    def get_detector_region(self, section):
        """Given a data region, get the detector section that this covers.
        This is the inverse of get_data section"""
        pass

    def copy_in(self, data):
        """
        Copy in the data from another CCDData object based on the detector sections

        :param data_to_copy:
        :return:
        """
        overlap_section = self.get_overlap(data.detector_section)
        data_to_copy = data.trim(data.get_data_section(overlap_section))
        data_to_copy = data_to_copy.rebin(self.binning)
        for array_name_to_copy in ['data', 'mask', 'uncertainty']:
            array_to_copy = getattr(data_to_copy, array_name_to_copy)
            my_overlap = self.get_data_section(overlap_section).to_slice()
            getattr(self, array_name_to_copy)[my_overlap].ravel()[:] = array_to_copy.ravel()[:]

    def init_poisson_uncertainties(self):
        self.uncertainty += np.sqrt(np.abs(self.data))


class Section:
    def __init__(self, x_start, x_stop, y_start, y_stop):
        """
        All 1 indexed inclusive (ala IRAF)
        :param x_start:
        :param x_stop:
        :param y_start:
        :param y_stop:
        """
        self.x_start = x_start
        self.x_stop = x_stop
        self.y_start = y_start
        self.y_stop = y_stop

    def to_slice(self):
        """
        Return a numpy-compatible pixel section
        """
        if None in [self.x_start, self.x_stop, self.y_start, self.y_stop]:
            return None

        y_slice = self._section_to_slice(self.y_start, self.y_stop)
        x_slice = self._section_to_slice(self.x_start, self.x_stop)

        return y_slice, x_slice

    def _section_to_slice(self, start, stop):
        """
        Given a start and stop pixel in IRAF coordinates, convert to a 
        numpy-compatible slice.
        """
        if stop > start:
            pixel_slice = slice(start - 1, stop, 1)
        else:
            if stop == 1:
                pixel_slice = slice(start - 1, None, -1)
            else:
                pixel_slice = slice(start - 1, stop - 2, -1)
        
        return pixel_slice

    @property
    def shape(self):
        return np.abs(self.y_stop - self.y_start) + 1, np.abs(self.x_stop - self.x_start) + 1

    def overlap(self, section):
        return Section(max(min(section.x_start, section.x_stop), min(self.x_start, self.x_stop)),
                       min(max(section.x_start, section.x_stop), max(self.x_start, self.x_stop)),
                       max(min(section.y_start, section.y_stop), min(self.y_start, self.y_stop)),
                       min(max(section.y_start, section.y_stop), max(self.y_start, self.y_stop)))

    @classmethod
    def parse_region_keyword(cls, keyword_value):
        """
        Convert a header keyword of the form [x1:x2],[y1:y2] into a Section object
        :param keyword_value: Header keyword string
        :return: x, y index slices
        """
        if not keyword_value:
            return cls(None, None, None, None)
        elif keyword_value.lower() == 'unknown':
            return cls(None, None, None, None)
        elif keyword_value.lower() == 'n/a':
            return cls(None, None, None, None)
        else:
            # Strip off the brackets and split the coordinates
            pixel_sections = keyword_value[1:-1].split(',')
            x_start, x_stop = pixel_sections[0].split(':')
            y_start, y_stop = pixel_sections[1].split(':')
        return cls(int(x_start), int(x_stop), int(y_start), int(y_stop))

    def to_region_keyword(self):
        return f'[{self.x_start}:{self.x_stop},{self.y_start}:{self.y_stop}]'



class Image(CCDData):
    pass

class Table(Data):
    pass


class ObservationFrame(metaclass=abc.ABCMeta):
    def __init__(self, hdu_list: list, file_path: str):
        self._hdus = hdu_list
        self._file_path = file_path
        self.epoch = self.primary_hdu.meta.get('DAY-OBS')
        self.instrument = None

    @property
    def primary_hdu(self):
        return self._hdus[0]

    @primary_hdu.setter
    def primary_hdu(self, hdu):
        if len(self._hdus) > 0:
            self._hdus.remove(self.primary_hdu)
        self._hdus.insert(0, hdu)

    @property
    def data(self):
        return self.primary_hdu.data

    @property
    def mask(self):
        return self.primary_hdu.mask

    @property
    def meta(self):
        return self.primary_hdu.meta

    @property
    def ccd_hdus(self):
        return [hdu for hdu in self._hdus if isinstance(hdu, CCDData)]

    @property
    def filename(self):
        return os.path.basename(self._file_path)

    def append(self, hdu):
        self._hdus.append(hdu)

    def remove(self, hdu):
        self._hdus.remove(hdu)

    def insert(self, index, hdu):
        self._hdus.insert(index, hdu)

    def subtract(self, value, kind=None):
        self.primary_hdu.subtract(value, kind=None)

    @abc.abstractmethod
    def get_output_filename(self, runtime_context) -> str:
        pass

    @property
    @abc.abstractmethod
    def obstype(self):
        pass

    @property
    @abc.abstractmethod
    def dateobs(self):
        pass

    @property
    @abc.abstractmethod
    def datecreated(self):
        pass

    @property
    def data_type(self):
        # Convert bytes to bits
        size = 8 * min([hdu.data.itemsize for hdu in self.ccd_hdus])
        if 'f' in [hdu.data.dtype.kind for hdu in self.ccd_hdus]:
            float_or_int = 'float'
        else:
            float_or_int = 'int'
        return getattr(np, f'{float_or_int}{size}')

    def write(self, runtime_context):
        output_filename = self.get_output_filename(runtime_context)
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        # TODO: Add option to write to AWS
        with open(output_filename, 'wb') as f:
            self.to_fits(fpack=runtime_context.fpack).writeto(f, overwrite=True)

    def to_fits(self, fpack=False):
        hdu_list_to_write = fits.HDUList([])
        for hdu in self._hdus:
            hdu_list_to_write += hdu.to_fits()
        if fpack:
            hdu_list_to_write = fits_utils.pack(hdu_list_to_write)
        return hdu_list_to_write

    @property
    def binning(self):
        # Get the smallest binnings in every extension
        x_binnings = []
        y_binnings = []
        for hdu in self.ccd_hdus:
            x_binning, y_binning = hdu.binning
            x_binnings.append(x_binning)
            y_binnings.append(y_binning)
        return [min(x_binnings), min(y_binnings)]


class CalibrationFrame(ObservationFrame, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_output_filename(self, runtime_context) -> str:
        pass

    def __init__(self, hdu_list: list, context):
        super().__init__(hdu_list, context)
        self.is_bad = False
        self.grouping_criteria = []

    @property
    def is_master(self):
        return self.meta.get('ISMASTER', False)

    @is_master.setter
    def is_master(self, value):
        self.meta['ISMASTER'] = value

    def write(self, runtime_context):
        super().write(runtime_context)
        # TODO: make sure we save the DB info if we need to.
        dbs.save_calibration_info(self.get_output_filename(runtime_context), self, runtime_context.db_address)


class LCOObservationFrame(ObservationFrame):
    # TODO: Set all status check keywords to bad if they are not already set
    # TODO: Add gain validation
    def get_output_filename(self, runtime_context):
        # TODO add a mode for AWS filenames
        output_directory = os.path.join(runtime_context.processed_path, self.instrument.site,
                                        self.instrument.name, self.epoch, 'processed')
        output_filename = self._file_path.replace('00.fits', '{:02d}.fits'.format(int(runtime_context.reduction_level)))
        output_filename = os.path.join(output_directory, os.path.basename(output_filename))
        if runtime_context.fpack and not output_filename.endswith('.fz'):
            output_filename += '.fz'
        return output_filename

    @property
    def obstype(self):
        return self.primary_hdu.meta.get('OBSTYPE')

    @property
    def dateobs(self):
        return Time(self.primary_hdu.meta.get('DATE-OBS'), scale='utc').datetime

    @property
    def datecreated(self):
        return Time(self.primary_hdu.meta.get('DATE'), scale='utc').datetime

    @property
    def configuration_mode(self):
        mode = self.meta.get('CONFMODE', 'default')
        if str(mode).lower() in ['n/a', '0', 'normal']:
            mode = 'default'
        return mode


class LCOCalibrationFrame(LCOObservationFrame, CalibrationFrame):
    pass


class LCOImageFactory:
    @classmethod
    def open(cls, path, runtime_context) -> ObservationFrame:
        fits_hdu_list = fits_utils.open_fits_file(path)
        hdu_list = [CCDData(data=hdu.data.astype(np.float32), meta=hdu.header, name=hdu.header.get('EXTNAME'))
                    if hdu.data is not None else HeaderOnly(meta=hdu.header) for hdu in fits_hdu_list]
        if hdu_list[0].meta.get('OBSTYPE') in runtime_context.CALIBRATION_IMAGE_TYPES:
            image = LCOCalibrationFrame(hdu_list, os.path.basename(path))
            image.grouping_criteria = runtime_context.CALIBRATION_SET_CRITERIA.get(image.obstype, [])
        else:
            image = LCOObservationFrame(hdu_list, os.path.basename(path))
        image.instrument = cls._get_instrument(image, runtime_context.db_address)

        # TODO: Put all munge code here
        for hdu in image.ccd_hdus:
            for keyword in ['SATURATE', 'MAXLIN']:
                hdu.meta[keyword] = hdu.meta.get(keyword, image.primary_hdu.meta[keyword])

        return image

    @classmethod
    def _get_instrument(cls, image, db_address):
        site = image.meta.get('SITEID')
        camera = image.meta.get('INSTRUME')
        enclosure = image.meta.get('ENCID')
        telescope = image.meta.get('TELID')
        instrument = dbs.query_for_instrument(db_address, site, camera, enclosure=enclosure, telescope=telescope)
        name = camera
        if instrument is None:
            # if instrument is missing, assume it is an NRES frame and check for the instrument again.
            name = image.meta.get('TELESCOP')
            instrument = dbs.query_for_instrument(db_address, site, camera, name=name, enclosure=None, telescope=None)
        if instrument is None:
            msg = 'Instrument is not in the database, Please add it before reducing this data.'
            tags = {'site': site, 'enclosure': enclosure,
                    'telescope': telescope, 'camera': camera, 'telescop': name}
            logger.error(msg, extra_tags=tags)
            raise ValueError('Instrument is missing from the database.')
        return instrument


class DataTable:
    pass

def regenerate_data_table_from_fits_hdu_list():
    pass
# class MasterCalibrationImage(ObservationalFrame):
#     # Sort the images by reverse observation date, so that the most recent one
#     # is used to create the filename and select the day directory
#     images.sort(key=lambda image: image.dateobs, reverse=True)
#     make_calibration_name = file_utils.make_calibration_filename_function(self.calibration_type,
#                                                                           self.runtime_context)
#     master_calibration_filename = make_calibration_name(images[0])
#     master_header = create_master_calibration_header(images[0].header, images)
#     master_image = FRAME_CLASS(self.runtime_context, data=stacked_data, header=master_header)
#     master_image.filename = master_calibration_filename
#     master_image.bpm = master_bpm
#     master_bpm = np.array(stacked_data == 0.0, dtype=np.uint8)
#
#     def create_master_calibration_header(old_header, images):
#         header = fits.Header()
#         for key in old_header.keys():
#             try:
#                 # Dump empty header keywords and ignore old histories.
#                 if len(key) > 0 and key != 'HISTORY' and key != 'COMMENT':
#                     for i in range(old_header.count(key)):
#                         header[key] = (old_header[(key, i)], old_header.comments[(key, i)])
#             except ValueError as e:
#                 logger.error('Could not add keyword {key}: {error}'.format(key=key, error=e))
#                 continue
#         header = fits_utils.sanitizeheader(header)
#         observation_dates = [image.dateobs for image in images]
#         mean_dateobs = date_utils.mean_date(observation_dates)
#
#         header['DATE-OBS'] = (date_utils.date_obs_to_string(mean_dateobs), '[UTC] Mean observation start time')
#         header['ISMASTER'] = (True, 'Is this a master calibration frame')
#
#         for i, image in enumerate(images):
#             header['IMCOM{:03d}'.format(i + 1)] = image.filename, 'Image combined to create master calibration image'
#         return header
#
#
# class CCDData:
#     def __init__(self, data, meta, mask=None, uncertainty=None):
#         self.data = data
#         self.meta = meta
#         self.mask = mask
#         self.uncertainty = uncertainty
#
#         # TODO raise an exception if gain is not valid
#         # TODO: Log a warning if header keywords are missing
#
#         # TODO: On setting the mask, raise an exception if it is the wrong size
#         # TODO: on load, check, header saturation, 1000's etc. Basically anything that makes the data dead on arrival
#
#         # TODO: add a bias_level ? subtract_bias_level, HEADER keyword('Mean bias level of master bias')
#         # TODO: add a save status method?
#         #master_bias_filename = os.path.basename(master_calibration_image.filename)
#         #image.header['L1IDBIAS'] = (master_bias_filename, 'ID of bias frame')
#         #image.header['L1STATBI'] = (1, "Status flag for bias frame correction")
#         # image.calibration_status['overscan'] = int(bias_region is None), 'Status flag for overscan correction')
#         # TODO: on write fill in any of the missing status keywords:
#         # TODO: add subtract_overscan?
#         # TODO: add trim
#         # TODO: load 3d data cubes into multiple extensions on creation
#         # TODO: add a copy_to function
#         # TODO: primary hdu setter needs to add primary data to image extension if the ccddata is an image type
#     def __iadd__(self, other):
#         self.data += other
#
#     def __isub__(self, other):
#         self.data -= other
#
#     def __imul__(self, other):
#         self.data *= other
#         self.meta['SATURATE'] *= other
#         self.meta['GAIN'] *= other
#         self.meta['MAXLIN'] *= other
#
#     def __idiv__(self, other):
#         self.data /= other
#         self.meta['SATURATE'] /= other
#         self.meta['GAIN'] /= other
#         self.meta['MAXLIN'] /= other
#
#     @property
#     def gain(self):
#         return self.meta['GAIN']
#
#     @gain.setter
#     def gain(self, value):
#         self.meta['GAIN'] = value
#
#     @property
#     def saturate(self):
#         return self.meta['SATURATE']
#
#     @saturate.setter
#     def saturate(self, value):
#         self.meta['SATURATE'] = value
#
#     def get_region(self, region_name):
#         pass
#
#     def _trim_image(region):
#         trimsec = fits_utils.parse_region_keyword(image.header['TRIMSEC'])
#
#         if trimsec is not None:
#             image.data = image.data[trimsec]
#             image.bpm = image.bpm[trimsec]
#
#             # Update the NAXIS and CRPIX keywords
#             image.header['NAXIS1'] = trimsec[1].stop - trimsec[1].start
#             image.header['NAXIS2'] = trimsec[0].stop - trimsec[0].start
#             if 'CRPIX1' in image.header:
#                 image.header['CRPIX1'] -= trimsec[1].start
#             if 'CRPIX2' in image.header:
#                 image.header['CRPIX2'] -= trimsec[0].start
#
#             image.header['L1STATTR'] = (1, 'Status flag for overscan trimming')
#         else:
#             logger.warning('TRIMSEC was not defined.', image=image, extra_tags={'trimsec': image.header['TRIMSEC']})
#             image.header['L1STATTR'] = (0, 'Status flag for overscan trimming')
#         return image.header['NAXIS1'], image.header['NAXIS2']
#
#
#     def get_mosaic_size(image, n_amps):
#         """
#         Get the necessary size of the output mosaic image
#
#         Parameters
#         ----------
#         image: banzai.images.Image
#                image (with extensions) to mosaic
#         n_amps: int
#                 number of amplifiers (fits extensions) in the image
#
#         Returns
#         -------
#         nx, ny: int, int
#                 The number of pixels in x and y that needed for the output mosaic.
#
#         Notes
#         -----
#         Astropy fits data arrays are indexed y, x.
#         """
#         ccdsum = image.ccdsum.split(' ')
#         x_pixel_limits, y_pixel_limits = get_detsec_limits(image, n_amps)
#         nx = (np.max(x_pixel_limits) - np.min(x_pixel_limits) + 1) // int(ccdsum[0])
#         ny = (np.max(y_pixel_limits) - np.min(y_pixel_limits) + 1) // int(ccdsum[1])
#         return nx, ny
#
#
#     def copy_to():
#         mosaiced_data = np.zeros((ny, nx), dtype=np.float32)
#         mosaiced_bpm = np.zeros((ny, nx), dtype=np.uint8)
#         x_detsec_limits, y_detsec_limits = get_detsec_limits(image, image.get_n_amps())
#         xmin = min(x_detsec_limits) - 1
#         ymin = min(y_detsec_limits) - 1
#         for i in range(image.get_n_amps()):
#             ccdsum = image.extension_headers[i].get('CCDSUM', image.ccdsum)
#             x_binning, y_binning = ccdsum.split(' ')
#             datasec = image.extension_headers[i]['DATASEC']
#             amp_slice = fits_utils.parse_region_keyword(datasec)
#
#             detsec = image.extension_headers[i]['DETSEC']
#             mosaic_slice = get_windowed_mosaic_slices(detsec, xmin, ymin, x_binning, y_binning)
#
#
#             mosaiced_data[mosaic_slice] = image.data[i][amp_slice]
#             mosaiced_bpm[mosaic_slice] = image.bpm[i][amp_slice]
#
#         image.data = mosaiced_data
#         image.bpm = mosaiced_bpm
#         # Flag any missing data
#         image.bpm[image.data == 0.0] = 1
#         image.update_shape(nx, ny)
#         update_naxis_keywords(image, nx, ny)
# class ImageData(CCDData):
#     # TODO: get requested and central coordinates (Image subclass?)
#     pass
#
#
# class Image(object):
#
#     def __init__(self, runtime_context, filename):
#         self._hdu_list = fits_utils.init_hdu()
#
#         # Amplifier specific (in principle)
#         self.data, self.header, extensions = fits_utils.open_image(filename)
#         self.readnoise = float(self.header.get('RDNOISE', 0.0))
#         if len(self.extension_headers) > 0 and 'GAIN' in self.extension_headers[0]:
#                 self.gain = [h['GAIN'] for h in self.extension_headers]
#         else:
#             self.gain = eval(str(self.header.get('GAIN')))
#         self.ccdsum = self.header.get('CCDSUM')
#         self.nx = self.header.get('NAXIS1')
#         self.ny = self.header.get('NAXIS2')
#
#         # Observation specific
#         self.filename = os.path.basename(filename)
#         self.request_number = self.header.get('REQNUM')
#         self.instrument = dbs.get_instrument(runtime_context)
#         self.epoch = str(self.header.get('DAY-OBS'))
#         self.configuration_mode = fits_utils.get_configuration_mode(self.header)
#         self.block_id = self.header.get('BLKUID')
#         self.block_start = date_utils.parse_date_obs(self.header.get('BLKSDATE', '1900-01-01T00:00:00.00000'))
#         self.molecule_id = self.header.get('MOLUID')
#         self.obstype = self.header.get('OBSTYPE')
#         self.dateobs = date_utils.parse_date_obs(self.header.get('DATE-OBS', '1900-01-01T00:00:00.00000'))
#         self.datecreated = date_utils.parse_date_obs(self.header.get('DATE', date_utils.date_obs_to_string(self.dateobs)))
#         self.exptime = float(self.header.get('EXPTIME', np.nan))
#
#         # Imaging specific
#         self.filter = self.header.get('FILTER')
#         self.ra, self.dec = fits_utils.parse_ra_dec(self.header)
#         self.pixel_scale = float(self.header.get('PIXSCALE', np.nan))
#         munge.munge(self)
#
#         # Calibrations?
#         self.is_bad = False
#         self.is_master = self.header.get('ISMASTER', False)
#         self.attributes = settings.CALIBRATION_SET_CRITERIA.get(self.obstype, {})
#
#         # What about NRES composite data products? Some extra extensions just need to go along for the ride
#
#     def __del__(self):
#         self._hdu_list.close()
#         self._hdu_list._file.close()
#
#     def write(self, runtime_context):
#         file_utils.save_pipeline_metadata(self.header, runtime_context.rlevel)
#         output_filename = file_utils.make_output_filename(self.filename, runtime_context.fpack, runtime_context.rlevel)
#         output_directory = file_utils.make_output_directory(runtime_context.processed_path, self.instrument.site,
#                                                             self.instrument.name, self.epoch,
#                                                             preview_mode=runtime_context.preview_mode)
#         filepath = os.path.join(output_directory, output_filename)
#         fits_utils.write_fits_file(filepath, self._hdu_list, runtime_context)
#         if self.obstype in settings.CALIBRATION_IMAGE_TYPES:
#             dbs.save_calibration_info(filepath, self, db_address=runtime_context.db_address)
#         if runtime_context.post_to_archive:
#             file_utils.post_to_archive_queue(filepath, runtime_context.broker_url)
#
#     def add_fits_extension(self, extension):
#         self._hdu_list.append(extension)
#
#     def update_shape(self, nx, ny):
#         self.nx = nx
#         self.ny = ny
#
#     def data_is_3d(self):
#         return len(self.data.shape) > 2
#
#     def get_n_amps(self):
#         if self.data_is_3d():
#             n_amps = self.data.shape[0]
#         else:
#             n_amps = 1
#         return n_amps
#
#     def get_inner_image_section(self, inner_edge_width=0.25):
#         """
#         Extract the inner section of the image with dimensions:
#         ny * inner_edge_width * 2.0 x nx * inner_edge_width * 2.0
#
#         Parameters
#         ----------
#
#         inner_edge_width: float
#                           Size of inner edge as fraction of total image size
#
#         Returns
#         -------
#         inner_section: array
#                        Inner section of image
#         """
#         if self.data_is_3d():
#             logger.error("Cannot get inner section of a 3D image", image=self)
#             raise ValueError
#
#         inner_nx = round(self.nx * inner_edge_width)
#         inner_ny = round(self.ny * inner_edge_width)
#         return self.data[inner_ny: -inner_ny, inner_nx: -inner_nx]
