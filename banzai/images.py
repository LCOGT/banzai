import logging
from banzai import dbs
from banzai.utils import fits_utils, stats, date_utils, image_utils
import numpy as np
from astropy.io import fits
from astropy.table import Table
from astropy.time import Time
from typing import Union, Type
import tempfile
import abc
import os
from typing import Optional
from fnmatch import fnmatch
import datetime

logger = logging.getLogger('banzai')


class Data(metaclass=abc.ABCMeta):
    _file_handles = []

    def __init__(self, data: Union[np.array, Table], meta: Union[dict, fits.Header],
                 mask: np.array = None, name: str = ''):
        self.data = self._init_array(data)
        self.meta = meta.copy()
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
        return self.meta.get('EXTNAME')

    @extension_name.setter
    def extension_name(self, value):
        self.meta['EXTNAME'] = value

    @classmethod
    def from_fits(cls, hdu: Union[fits.ImageHDU, fits.TableHDU, fits.BinTableHDU]):
        return cls(hdu.data, hdu.header, name=hdu.header.get('EXTNAME'))

    @abc.abstractmethod
    def to_fits(self, context) -> Union[fits.HDUList, list]:
        pass


class HeaderOnly(Data):
    def __init__(self, meta: Union[dict, fits.Header]):
        super().__init__(data=np.zeros(0), meta=meta)

    def to_fits(self, context):
        return fits.HDUList([fits.ImageHDU(data=None, header=self.meta)])


class DataTable(Data):
    def __init__(self, data, meta, name):
        super().__init__(data, meta, name=name)

    def to_fits(self, context) -> Union[fits.HDUList, list]:
        return [fits.BinTableHDU(data=self.data, header=self.meta)]


class ArrayData(Data):
    def __init__(self, data, meta, name):
        super().__init__(data, meta, name=name)

    def to_fits(self, context) -> Union[fits.HDUList, list]:
        return [fits.ImageHDU(data=self.data, header=self.meta)]


class CCDData(Data):
    def __init__(self, data: Union[np.array, Table], meta: Union[dict, fits.Header],
                 mask: np.array = None, name: str = '', uncertainty: np.array = None):
        super().__init__(data=data, meta=meta, mask=mask, name=name)
        if uncertainty is None:
            uncertainty = self.read_noise * np.ones(data.shape, dtype=data.dtype) / self.gain
        self.uncertainty = self._init_array(uncertainty)

    def __getitem__(self, section):
        """
        Return a new CCDData object with the given section of data
        :param section: needs to be  in data coords
        :return:
        """
        return self.trim(trim_section=section)

    def __imul__(self, value):
        self.data *= value
        self.uncertainty *= value
        self.meta['SATURATE'] *= value
        self.meta['GAIN'] *= value
        self.meta['MAXLIN'] *= value
        return self

    def __itruediv__(self, value):
        if isinstance(value, CCDData):
            self.data /= value.data
            self.uncertainty /= value.uncertainty
            # TODO: is this correct for flat normalization?
            # self.meta['SATURATE'] /= value.meta['SATURATE']
            # self.meta['GAIN'] /= value.meta['GAIN']
            # self.meta['MAXLIN'] /= value.meta['MAXLIN']
        else:
            self.__imul__(1.0 / value)
        return self

    def to_fits(self, context):
        data_hdu = fits.ImageHDU(data=self.data, header=fits.Header(self.meta))
        bpm_extname = self.extension_name + 'BPM'
        for extname in context.EXTENSION_NAMES_TO_CONDENSE:
            bpm_extname = bpm_extname.replace(extname, '')
        mask_hdu = fits.ImageHDU(data=self.mask, header=fits.Header({'EXTNAME': bpm_extname}))
        uncertainty_extname = self.extension_name + 'ERR'
        for extname in context.EXTENSION_NAMES_TO_CONDENSE:
            uncertainty_extname = uncertainty_extname.replace(extname, '')
        uncertainty_hdu = fits.ImageHDU(data=self.uncertainty, header=fits.Header({'EXTNAME': uncertainty_extname}))
        hdulist = fits.HDUList([data_hdu, mask_hdu, uncertainty_hdu])
        return hdulist

    def __del__(self):
        super().__del__()
        del self.uncertainty

    def __isub__(self, value):
        if isinstance(value, CCDData):
            self.data -= value.data
            self.uncertainty = np.sqrt(value.uncertainty * value.uncertainty + self.uncertainty * self.uncertainty)
            self.mask |= value.mask
        else:
            self.data -= value
        return self

    def __sub__(self, other):
        uncertainty = np.sqrt(self.uncertainty * self.uncertainty + other.uncertainty * other.uncertainty)
        return type(self)(data=self.data - other.data, meta=self.meta, mask=self.mask|other.mask,
                          uncertainty=uncertainty)

    def signal_to_noise(self):
        return np.abs(self.data) / self.uncertainty

    def get_overscan_region(self):
        return Section.parse_region_keyword(self.meta.get('BIASSEC', 'N/A'))

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
        inner_nx = round(self.data.shape[1] * inner_edge_width)
        inner_ny = round(self.data.shape[0] * inner_edge_width)
        return self.data[inner_ny: -inner_ny, inner_nx: -inner_nx]

    def trim(self, trim_section=None):
        """
        :param trim_section: Always in data coords
        :return:
        """
        if trim_section is None:
            trim_section = Section.parse_region_keyword(self.meta.get('TRIMSEC', 'N/A'))

        trimmed_image = type(self)(data=self.data[trim_section.to_slice()], meta=self.meta,
                                   mask=self.mask[trim_section.to_slice()], name=self.name,
                                   uncertainty=self.uncertainty[trim_section.to_slice()])
        trimmed_image.detector_section = self.data_to_detector_section(trim_section)
        trimmed_image._data_section = Section(x_start=1, y_start=1,
                                              x_stop=trimmed_image.data.shape[1],
                                              y_stop=trimmed_image.data.shape[0])
        return trimmed_image

    @property
    def dtype(self):
        return self.data.dtype

    @property
    def shape(self):
        return self.data.shape

    @property
    def gain(self):
        return self.meta.get('GAIN', 1.0)

    @gain.setter
    def gain(self, value):
        self.meta['GAIN'] = value

    @property
    def saturate(self):
        return self.meta.get('SATURATE')

    @saturate.setter
    def saturate(self, value):
        self.meta['SATURATE'] = value

    @property
    def max_linearity(self):
        return self.meta.get('MAXLIN')

    @max_linearity.setter
    def max_linearity(self, value):
        self.meta['MAXLIN'] = value

    @property
    def read_noise(self):
        return self.meta.get('RDNOISE', 0.0)

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

    def detector_to_data_section_oned(self, axis, section):
        binning_indices = {'x': 0, 'y': 1}
        sign = np.sign(getattr(self.detector_section, f'{axis}_stop') - getattr(self.detector_section, f'{axis}_start'))
        sign *= np.sign(getattr(self._data_section, f'{axis}_stop') - getattr(self._data_section, f'{axis}_start'))

        start = sign * (getattr(section, f'{axis}_start') - getattr(self.detector_section, f'{axis}_start'))
        start //= self.binning[binning_indices[axis]]
        start += getattr(self._data_section, f'{axis}_start')

        stop = sign * (getattr(section, f'{axis}_stop') - getattr(self.detector_section, f'{axis}_start'))
        stop //= self.binning[binning_indices[axis]]
        stop += getattr(self._data_section, f'{axis}_start')
        return start, stop

    def detector_to_data_section(self, section):
        """Given a detector region, figure out the corresponding data region
        Note the + and - 1 factors cancel
        Really this is just doing the same thing as a CD matrix calculation
        r_data - data_0 = M (r_det - det_0) where M is a transformation matrix and everything else is a 2d vector
        M is either the 1/binning * identity or the 1/binning * negative Identity depending on if datasec and detsec
        are in the same ordering (both increasing or both decreasing gives positive)
        Note if you want to add a rotation, M can contain that as well.
        """
        x_start, x_stop = self.detector_to_data_section_oned('x', section)
        y_start, y_stop = self.detector_to_data_section_oned('y', section)

        return Section(x_start, x_stop, y_start, y_stop)

    def data_to_detector_section(self, section):
        """Given a data region, get the detector section that this covers.
        This is the inverse of get_data section"""
        x_start, x_stop = self.data_to_detector_section_oned('x', section)
        y_start, y_stop = self.data_to_detector_section_oned('y', section)

        return Section(x_start, x_stop, y_start, y_stop)

    def data_to_detector_section_oned(self, axis, section):
        binning_indices = {'x': 0, 'y': 1}
        sign = np.sign(getattr(self.detector_section, f'{axis}_stop') - getattr(self.detector_section, f'{axis}_start'))
        sign *= np.sign(getattr(self._data_section, f'{axis}_stop') - getattr(self._data_section, f'{axis}_start'))

        start = sign * (getattr(section, f'{axis}_start') - getattr(self._data_section, f'{axis}_start'))
        start *= self.binning[binning_indices[axis]]
        start += getattr(self.detector_section, f'{axis}_start')

        stop = sign * (getattr(section, f'{axis}_stop') - getattr(self._data_section, f'{axis}_start'))
        stop *= self.binning[binning_indices[axis]]
        stop += getattr(self.detector_section, f'{axis}_start')

        # when converting from binned to unbinned coordinates, we need to adjust our stopping pixel
        # by (binning - 1)
        stop += np.sign(stop - start) * (self.binning[binning_indices[axis]] - 1)

        return start, stop

    def copy_in(self, data):
        """
        Copy in the data from another CCDData object based on the detector sections

        :param data_to_copy:
        :return:
        """
        overlap_section = self.get_overlap(data.detector_section)
        data_to_copy = data.trim(trim_section=data.detector_to_data_section(overlap_section))
        data_to_copy = data_to_copy.rebin(self.binning)
        for array_name_to_copy in ['data', 'mask', 'uncertainty']:
            array_to_copy = getattr(data_to_copy, array_name_to_copy)
            my_overlap = self.detector_to_data_section(overlap_section).to_slice()
            getattr(self, array_name_to_copy)[my_overlap][:] = array_to_copy[:]

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
    def shape(self):
        return self.primary_hdu.data.shape

    @property
    def data(self):
        return self.primary_hdu.data

    @property
    def mask(self):
        return self.primary_hdu.mask

    @mask.setter
    def mask(self, mask):
        self.primary_hdu.mask = mask

    @property
    def meta(self):
        return self.primary_hdu.meta

    @property
    def ccd_hdus(self):
        return [hdu for hdu in self._hdus if isinstance(hdu, CCDData)]

    @property
    def filename(self):
        return os.path.basename(self._file_path)

    @abc.abstractmethod
    def save_processing_metadata(self, context):
        pass

    @property
    @abc.abstractmethod
    def bias_level(self):
        pass

    @bias_level.setter
    @abc.abstractmethod
    def bias_level(self, value):
        pass

    def append(self, hdu):
        self._hdus.append(hdu)

    def remove(self, hdu):
        self._hdus.remove(hdu)

    def replace(self, old_data, new_data):
        self._hdus.insert(self._hdus.index(old_data), new_data)
        self._hdus.remove(old_data)

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
    @abc.abstractmethod
    def exptime(self):
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
        self.save_processing_metadata(runtime_context)
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        # TODO: Add option to write to AWS
        with open(output_filename, 'wb') as f:
            self.to_fits(runtime_context).writeto(f, overwrite=True, output_verify='silentfix')
        dbs.save_processed_image(output_filename, db_address=runtime_context.db_address)

    def to_fits(self, context):
        hdu_list_to_write = fits.HDUList([])
        for hdu in self._hdus:
            hdu_list_to_write += hdu.to_fits(context)
        if not isinstance(hdu_list_to_write[0], fits.PrimaryHDU):
            hdu_list_to_write[0] = fits.PrimaryHDU(data=hdu_list_to_write[0].data, header=hdu_list_to_write[0].header)
        if context.fpack:
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

    def __sub__(self, other):
        return self.primary_hdu - other.primary_hdu

    def __isub__(self, other):
        if isinstance(other, ObservationFrame):
            self.primary_hdu.__isub__(other.primary_hdu)
        else:
            self.primary_hdu.__isub__(other)
        return self

    def __imul__(self, other):
        if isinstance(other, ObservationFrame):
            self.primary_hdu.__imul__(other.primary_hdu)
        else:
            self.primary_hdu.__imul__(other)
        return self

    def __itruediv__(self, other):
        if isinstance(other, ObservationFrame):
            self.primary_hdu.__itruediv__(other.primary_hdu)
        else:
            self.primary_hdu.__itruediv__(other)
        return self


class CalibrationFrame(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_output_filename(self, runtime_context) -> str:
        pass

    def __init__(self, grouping_criteria: list = None):
        self.is_bad = False
        if grouping_criteria is None:
            grouping_criteria = []
        self.grouping_criteria = grouping_criteria

    @property
    @abc.abstractmethod
    def is_master(self):
        pass

    @is_master.setter
    @abc.abstractmethod
    def is_master(self, value):
        pass

    def write(self, runtime_context):
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
    def filter(self):
        return self.primary_hdu.meta.get('FILTER')

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

    @property
    def bias_level(self):
        return self.primary_hdu.meta.get('BIASLVL')

    @bias_level.setter
    def bias_level(self, value):
        self.primary_hdu.meta['BIASLVL'] = value

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
    def __init__(self, hdu_list: list, file_path: str, grouping_criteria: list = None):
        CalibrationFrame.__init__(self, grouping_criteria=grouping_criteria)
        LCOObservationFrame.__init__(self, hdu_list, file_path)

    @property
    def is_master(self):
        return self.meta.get('ISMASTER', False)

    @is_master.setter
    def is_master(self, value):
        self.meta['ISMASTER'] = value

    def write(self, runtime_context):
        LCOObservationFrame.write(self, runtime_context)
        CalibrationFrame.write(self, runtime_context)


class LCOMasterCalibrationFrame(LCOCalibrationFrame):
    def __init__(self, images: list, file_path: str, grouping_criteria: list = None):
        super().__init__(images, file_path, grouping_criteria=grouping_criteria)
        self._hdus = [CCDData(data=np.zeros(images[0].data.shape, dtype=images[0].data.dtype),
                           meta=self._create_master_calibration_header(images[0].meta, images))]
        self.is_master = True
        self.instrument = images[0].instrument

    def _create_master_calibration_header(self, old_header, images):
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
        header = fits_utils.sanitizeheader(header)
        observation_dates = [image.dateobs for image in images]
        mean_dateobs = date_utils.mean_date(observation_dates)

        header['DATE-OBS'] = (date_utils.date_obs_to_string(mean_dateobs), '[UTC] Mean observation start time')
        header['ISMASTER'] = (True, 'Is this a master calibration frame')

        header.add_history("Images combined to create master calibration image:")
        for i, image in enumerate(images):
            header[f'IMCOM{i+1:03d}'] = (image.filename, 'Image combined to create master')
        return header


class LCOFrameFactory:
    observation_frame_class = LCOObservationFrame
    calibration_frame_class = LCOCalibrationFrame
    @classmethod
    def open(cls, path, runtime_context) -> Optional[ObservationFrame]:
        fits_hdu_list = fits_utils.open_fits_file(path)
        hdu_list = []
        if fits_hdu_list[0].header.get('OBSTYPE').lower() == 'bpm' or \
                all('BPM' == hdu.header.get('EXTNAME', '') for hdu in fits_hdu_list if hdu.data is not None):
            for hdu in fits_hdu_list:
                if hdu.data is None:
                    hdu_list.append(HeaderOnly(meta=hdu.header))
                else:
                    hdu_list.append(CCDData(data=hdu.data, meta=hdu.header, name=hdu.header.get('EXTNAME')))

        for hdu in fits_hdu_list:
            # Move on from the BPM and ERROR arrays
            if 'BPM' in hdu.header.get('EXTNAME', '') or 'ERR' in hdu.header.get('EXTNAME', ''):
                continue
            if hdu.data is None:
                hdu_list.append(HeaderOnly(meta=hdu.header))
            elif isinstance(hdu, fits.BinTableHDU):
                hdu_list.append(DataTable(data=hdu.data, meta=hdu.header, name=hdu.header.get('EXTNAME')))
            elif 'GAIN' in hdu.header:
                condensed_name = hdu.header.get('EXTNAME', '')
                for extension_name_to_condense in runtime_context.EXTENSION_NAMES_TO_CONDENSE:
                    condensed_name = condensed_name.replace(extension_name_to_condense, '')
                if (condensed_name + 'BPM', hdu.header.get('EXTVER')) in fits_hdu_list:
                    bpm_array = fits_hdu_list[condensed_name + 'BPM', hdu.header.get('EXTVER')].data
                else:
                    bpm_array = None

                if (condensed_name + 'ERR', hdu.header.get('EXTVER')) in fits_hdu_list:
                    error_array = fits_hdu_list[condensed_name + 'ERR', hdu.header.get('EXTVER')].data
                else:
                    error_array = None
                if hdu.data.dtype == np.uint16:
                    hdu.data = hdu.data.astype(np.float32)
                hdu_list.append(CCDData(data=hdu.data, meta=hdu.header, name=hdu.header.get('EXTNAME'),
                                        mask=bpm_array, uncertainty=error_array))
            else:
                hdu_list.append(ArrayData(data=hdu.data, meta=hdu.header, name=hdu.header.get('EXTNAME')))
        if hdu_list[0].meta.get('OBSTYPE') in runtime_context.CALIBRATION_IMAGE_TYPES:
            grouping = runtime_context.CALIBRATION_SET_CRITERIA.get(hdu_list[0].meta.get('OBSTYPE'), [])
            image = cls.calibration_frame_class(hdu_list, os.path.basename(path), grouping_criteria=grouping)
        else:
            image = cls.observation_frame_class(hdu_list, os.path.basename(path))
        image.instrument = cls._get_instrument(image, runtime_context.db_address)

        # TODO: Put all munge code here

        for hdu in image.ccd_hdus:
            if hdu.meta.get('DETSEC', 'UNKNOWN') in ['UNKNOWN', 'N/A']:
                # DETSEC missing?
                binning = hdu.meta.get('CCDSUM', image.primary_hdu.meta.get('CCDSUM', '1 1'))
                data_section = Section.parse_region_keyword(hdu.meta['DATASEC'])
                detector_section = Section(1,
                                           max(data_section.x_start, data_section.x_stop) * int(binning[0]),
                                           1,
                                           max(data_section.y_start, data_section.y_stop) * int(binning[2]))
                hdu.meta['DETSEC'] = detector_section.to_region_keyword()

            # SATURATE Missing?
            def update_saturate(image, hdu, default):
                if hdu.meta.get('SATURATE', 0.0) == 0.0:
                    hdu.meta['SATURATE'] = image.meta.get('SATURATE', 0.0)
                    hdu.meta['MAXLIN'] = image.meta.get('MAXLIN', 0.0)
                if hdu.meta.get('SATURATE', 0.0) == 0.0:
                    hdu.meta['SATURATE'] = (default, '[ADU] Saturation level used')
                    hdu.meta['MAXLIN'] = (default, '[ADU] Non-linearity level')
            if 'sinistro' in image.instrument.type.lower():
                update_saturate(image, hdu, 47500.0)

            elif '1m0' in image.instrument.type:
                # Saturation level from ORAC Pipeline
                update_saturate(image, hdu, 46000.0)
            elif '0m4' in image.instrument.type or '0m8' in image.instrument.type:
                # Measured by Daniel Harbeck
                update_saturate(image, hdu, 64000.0)

            elif 'spectral' in image.instrument.type.lower():
                # These values were given by Joe Tufts on 2016-06-07
                binning = hdu.meta.get('CCDSUM', '1 1')
                n_binned_pixels = int(binning[0]) * int(binning[2])
                update_saturate(image, hdu, 125000.0 * n_binned_pixels / float(hdu.meta['GAIN']))
        if image_utils.image_can_be_processed(image, runtime_context):
            return image
        else:
            return None

    @classmethod
    def _get_instrument(cls, image, db_address):
        site = image.meta.get('SITEID')
        camera = image.meta.get('INSTRUME')
        instrument = dbs.query_for_instrument(db_address, site, camera)
        name = camera
        if instrument is None:
            # if instrument is missing, assume it is an NRES frame and check for the instrument again.
            name = image.meta.get('TELESCOP')
            instrument = dbs.query_for_instrument(db_address, site, camera, name=name)
        if instrument is None:
            msg = 'Instrument is not in the database, Please add it before reducing this data.'
            tags = {'site': site, 'camera': camera, 'telescop': name}
            logger.error(msg, extra_tags=tags)
            raise ValueError('Instrument is missing from the database.')
        return instrument


def stack(data_to_stack, nsigma_reject) -> CCDData:
    """
    """
    shape3d = [len(data_to_stack)] + list(data_to_stack[0].shape)
    a = np.zeros(shape3d, dtype=data_to_stack[0].dtype)
    uncertainties = np.zeros(shape3d, dtype=data_to_stack[0].dtype)
    mask = np.zeros(shape3d, dtype=np.uint8)

    for i, data in enumerate(data_to_stack):
        a[i, :, :] = data.data[:, :]
        mask[i, :, :] = data.mask[:, :]
        uncertainties[i, :, :] = data.uncertainty[:, :]

    abs_deviation = stats.absolute_deviation(a, axis=0, mask=mask)

    robust_std = stats.robust_standard_deviation(a, axis=0, abs_deviation=abs_deviation, mask=mask)

    robust_std = np.expand_dims(robust_std, axis=0)

    # Mask values that are N sigma from the median
    sigma_mask = abs_deviation > (nsigma_reject * robust_std)

    mask3d = np.logical_or(sigma_mask, mask > 0)
    n_good_pixels = np.logical_not(mask3d).sum(axis=0)

    stacked_mask = np.zeros(n_good_pixels.shape, dtype=np.uint8)

    # If a pixel is bad in all images, make sure we don't divide by zero
    bad_pixels = n_good_pixels == 0

    # If pixel is bad in all of the images, we take the logical or of all of the bits to go in the final mask
    stacked_mask[bad_pixels] = np.bitwise_or.reduce(mask, axis=0)[bad_pixels]

    # If a pixel is bad in all images, fill that pixel with the mean from the images
    n_good_pixels[bad_pixels] = len(data_to_stack)
    mask3d[:, bad_pixels] = False

    a[mask3d] = 0.0
    stacked_data = a.sum(axis=0) / n_good_pixels

    # Again if a pixel is bad in all images, fill the uncertainties with the quadrature sum / N images
    uncertainties[mask3d] = 0.0
    uncertainties *= uncertainties
    uncertainties /= n_good_pixels ** 2.0
    stacked_uncertainty = np.sqrt(uncertainties.sum(axis=0))

    return CCDData(data=stacked_data, meta=data_to_stack[0].meta, uncertainty=stacked_uncertainty, mask=stacked_mask)


def regenerate_data_table_from_fits_hdu_list():
    pass
