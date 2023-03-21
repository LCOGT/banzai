import abc
import tempfile
from typing import Union, Type

import numpy as np
from astropy.io import fits
from astropy.table import Table

from banzai.utils.image_utils import Section
from banzai.utils import fits_utils, stats
from io import BytesIO


class DataProduct:
    def __init__(self, file_buffer: BytesIO, filename: str, filepath: str = None,
                 meta: dict = None, frame_id: int = None):
        self.file_buffer = file_buffer
        self.filename = filename
        self.filepath = filepath
        self.meta = meta
        self.frame_id = frame_id

    @classmethod
    def from_fits(cls, hdu: fits.HDUList, filename: str, file_path: str):
        buffer = BytesIO()
        hdu.writeto(buffer)
        buffer.seek(0)
        return cls(buffer, filename, filepath=file_path)


class Data(metaclass=abc.ABCMeta):
    _file_handles = []

    def __init__(self, data: Union[np.array, Table], meta: Union[dict, fits.Header],
                 mask: np.array = None, name: str = '', memmap=True):
        self.memmap = memmap
        if isinstance(data, Table):
            self.data = data
        else:
            self.data = self._init_array(data)
        self.meta = meta.copy()
        self._validate_array(mask)
        self.mask = self._init_array(mask, dtype=np.uint8)
        self.name = name

    def _validate_array(self, array_data):
        if array_data is not None:
            if array_data.shape != self.data.shape:
                raise ValueError('Incoming array data must have the same dimensions as the image data')

    def _init_array(self, array: np.array = None, dtype: Type = None):
        if not self.memmap:
            return array
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
        self._validate_array(mask)
        self.mask = self._init_array(mask)

    def __del__(self):
        for handle in self._file_handles:
            handle.close()
        del self.data
        del self.mask

    @classmethod
    def from_fits(cls, hdu: Union[fits.ImageHDU, fits.TableHDU, fits.BinTableHDU]):
        return cls(hdu.data, hdu.header, name=hdu.header.get('EXTNAME'))

    @abc.abstractmethod
    def to_fits(self, context) -> Union[fits.HDUList, list]:
        pass


class HeaderOnly(Data):
    def __init__(self, meta: Union[dict, fits.Header]):
        super().__init__(data=np.zeros(0), meta=meta, memmap=False)

    def to_fits(self, context):
        return fits.HDUList([fits.ImageHDU(data=None, header=self.meta)])


class DataTable(Data):
    def __init__(self, data, name, meta=None, memmap=False):
        if meta is None:
            meta = fits.Header({})
        super().__init__(data, meta, name=name, memmap=memmap)

    def to_fits(self, context) -> Union[fits.HDUList, list]:
        hdu = fits.BinTableHDU(self.data, header=self.meta)
        hdu.name = self.name
        # For all TTYPE header keywords, set the header comment
        # from the table column's description.
        for k in self.meta.keys():
            if 'TTYPE' in k:
                column_name = self.meta[k]
                description = self.data[column_name].description
                hdu.header[k] = (column_name.upper(), description)
                # Get the value of n in TTYPEn
                n = k[5:]
                # Also add the TCOMMn header keyword with the description of the table column
                hdu.header['TCOMM{0}'.format(n)] = description

        return [hdu]


class ArrayData(Data):
    def __init__(self, data, name, meta=None, memmap=True):
        if meta is None:
            meta = fits.Header({})

        super().__init__(data, meta, name=name, memmap=memmap)

    def to_fits(self, context) -> Union[fits.HDUList, list]:
        return [fits.ImageHDU(data=self.data, header=fits.Header(self.meta), name=self.name)]


class CCDData(Data):
    def __init__(self, data: Union[np.array, Table], meta: fits.Header,
                 mask: np.array = None, name: str = '', uncertainty: np.array = None, memmap=True):
        super().__init__(data=data, meta=meta, mask=mask, name=name, memmap=memmap)
        if uncertainty is None:
            uncertainty = self.read_noise * np.ones(data.shape, dtype=data.dtype) / self.gain
        self.uncertainty = self._init_array(uncertainty)
        self._detector_section = Section.parse_region_keyword(self.meta.get('DETSEC'))
        self._data_section = Section.parse_region_keyword(self.meta.get('DATASEC'))
        self._background = None

    def __getitem__(self, section):
        """
        Return a new CCDData object with the given section of data
        :param section: needs to be  in data coords
        :return:
        """
        return self.trim(trim_section=section)

    def __imul__(self, value):
        # TODO: Handle the case where this is an array. Add SATURATE and GAIN handling when array.
        self.data *= value
        self.uncertainty *= value
        self.meta['EGAIN'] /= value
        return self

    def __itruediv__(self, value):
        if isinstance(value, CCDData):
            self.uncertainty = np.abs(self.data / value.data) * \
                               np.sqrt((self.uncertainty / self.data) ** 2 + (value.uncertainty / value.data) ** 2)
            self.data /= value.data
            self.mask |= value.mask
        else:
            self.__imul__(1.0 / value)
        return self

    def to_fits(self, context):
        data_hdu = fits.ImageHDU(data=self.data, header=fits.Header(self.meta), name=self.name)
        bpm_hdu = fits_utils.to_fits_image_extension(self.mask, self.name, 'BPM', context,
                                                     extension_version=self.meta.get('EXTVER'))
        uncertainty_hdu = fits_utils.to_fits_image_extension(self.uncertainty, self.name, 'ERR', context,
                                                             extension_version=self.meta.get('EXTVER'))
        hdulist = fits.HDUList([data_hdu, bpm_hdu, uncertainty_hdu])
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

    def add_uncertainty(self, readnoise: np.array):
        self._validate_array(readnoise)
        self.uncertainty = self._init_array(readnoise)

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
                                   uncertainty=self.uncertainty[trim_section.to_slice()], memmap=self.memmap)
        trimmed_image.detector_section = self.data_to_detector_section(trim_section)
        trimmed_image.data_section = Section(x_start=1, y_start=1,
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
        return [int(b) for b in self.meta.get('BINNING', '1 1').split('x')]

    @binning.setter
    def binning(self, value):
        x_binning, y_binning = value
        self.meta['CCDSUM'] = f'{x_binning} {y_binning}'

    @property
    def detector_section(self):
        return self._detector_section

    @detector_section.setter
    def detector_section(self, section):
        self.meta['DETSEC'] = section.to_region_keyword()
        self._detector_section = section

    @property
    def data_section(self):
        return self._data_section

    @data_section.setter
    def data_section(self, section):
        self.meta['DATASEC'] = section.to_region_keyword()
        self._data_section = section

    def rebin(self, binning):
        # TODO: Implement me
        return self

    def get_overlap(self, detector_section):
        return self.detector_section.overlap(detector_section)

    def detector_to_data_section_oned(self, axis, section):
        binning_indices = {'x': 0, 'y': 1}
        sign = np.sign(getattr(self.detector_section, f'{axis}_stop') - getattr(self.detector_section, f'{axis}_start'))
        sign *= np.sign(getattr(self.data_section, f'{axis}_stop') - getattr(self.data_section, f'{axis}_start'))

        start = sign * (getattr(section, f'{axis}_start') - getattr(self.detector_section, f'{axis}_start'))
        start //= self.binning[binning_indices[axis]]
        start += getattr(self.data_section, f'{axis}_start')

        stop = sign * (getattr(section, f'{axis}_stop') - getattr(self.detector_section, f'{axis}_start'))
        stop //= self.binning[binning_indices[axis]]
        stop += getattr(self.data_section, f'{axis}_start')
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
        sign *= np.sign(getattr(self.data_section, f'{axis}_stop') - getattr(self.data_section, f'{axis}_start'))

        start = sign * (getattr(section, f'{axis}_start') - getattr(self.data_section, f'{axis}_start'))
        start *= self.binning[binning_indices[axis]]
        start += getattr(self.detector_section, f'{axis}_start')

        stop = sign * (getattr(section, f'{axis}_stop') - getattr(self.data_section, f'{axis}_start'))
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
        self.uncertainty = np.sqrt(self.uncertainty ** 2.0 + np.abs(self.data))

    @property
    def background(self):
        return self._background

    @background.setter
    def background(self, value):
        if self._background is not None:
            self.data += self._background
        self._background = value
        self.data -= self._background


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
    stacked_uncertainty = np.sqrt(uncertainties.sum(axis=0) / (n_good_pixels ** 2.0))

    return CCDData(data=stacked_data, meta=data_to_stack[0].meta, uncertainty=stacked_uncertainty, mask=stacked_mask)
