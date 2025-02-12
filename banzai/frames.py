from banzai import dbs
from banzai.data import HeaderOnly, CCDData
from banzai.utils import fits_utils, file_utils
import numpy as np
from astropy.io import fits
import abc
import os
from typing import Optional
from banzai.logs import get_logger

logger = get_logger()


class ObservationFrame(metaclass=abc.ABCMeta):
    def __init__(self, hdu_list: list, file_path: str, frame_id: int = None, hdu_order: list = None):
        self._hdus = hdu_list
        self._file_path = file_path
        self.ra, self.dec = fits_utils.parse_ra_dec(hdu_list[0].meta)
        self.instrument = None
        self.frame_id = frame_id
        self.hdu_order = hdu_order
        self._hdu_mapping = {hdu.name: i for i, hdu in enumerate(hdu_list)}

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
        if isinstance(self.primary_hdu, HeaderOnly):
            return self.ccd_hdus[0].data
        else:
            return self.primary_hdu.data

    @property
    def mask(self):
        return self.primary_hdu.mask

    @mask.setter
    def mask(self, mask):
        self.primary_hdu.mask = mask

    @property
    def uncertainty(self):
        return self.primary_hdu.uncertainty

    @uncertainty.setter
    def uncertainty(self, uncertainty):
        self.primary_hdu.uncertainty = uncertainty

    @property
    def meta(self):
        return self.primary_hdu.meta

    @property
    def ccd_hdus(self):
        return [hdu for hdu in self._hdus if isinstance(hdu, CCDData)]

    @property
    def filename(self):
        return os.path.basename(self._file_path)

    @property
    def background(self):
        return self.primary_hdu.background

    @background.setter
    def background(self, value):
        self.primary_hdu.background = value

    @property
    def n_sub_exposures(self):
        return self.primary_hdu.n_sub_exposures

    @n_sub_exposures.setter
    def n_sub_exposures(self, value):
        self.primary_hdu.n_sub_exposures = value

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

    def add_or_update(self, hdu):
        if hdu.name not in self._hdu_mapping:
            self._hdus.append(hdu)
            i = len(self._hdus) - 1
            self._hdu_mapping[hdu.name] = i
        else:
            del self._hdus[self._hdu_mapping[hdu.name]]
            self._hdus.insert(self._hdu_mapping[hdu.name], hdu)

    def remove(self, hdu):
        self._hdus.remove(hdu)

    def replace(self, old_data, new_data):
        self._hdus.insert(self._hdus.index(old_data), new_data)
        self._hdus.remove(old_data)

    @abc.abstractmethod
    def get_output_directory(self, runtime_context) -> str:
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
    @abc.abstractmethod
    def epoch(self):
        pass

    @property
    @abc.abstractmethod
    def request_number(self):
        pass

    @property
    @abc.abstractmethod
    def ccd_temperature(self):
        pass

    @property
    @abc.abstractmethod
    def requested_ccd_temperature(self):
        pass

    @property
    @abc.abstractmethod
    def measured_ccd_temperature(self):
        pass

    @property
    @abc.abstractmethod
    def block_end_date(self):
        pass

    @property
    @abc.abstractmethod
    def proposal(self):
        pass

    @proposal.setter
    @abc.abstractmethod
    def proposal(self, value):
        pass

    @property
    @abc.abstractmethod
    def blockid(self):
        pass

    @property
    @abc.abstractmethod
    def public_date(self):
        pass

    @public_date.setter
    @abc.abstractmethod
    def public_date(self, value):
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

    @abc.abstractmethod
    def write(self, runtime_context):
        pass

    def to_fits(self, context):
        hdu_list_to_write = fits.HDUList([])
        for hdu in self._hdus:
            hdu_list_to_write += hdu.to_fits(context)
        fits_utils.reorder_hdus(hdu_list_to_write, self.hdu_order)
        if not isinstance(hdu_list_to_write[0], fits.PrimaryHDU):
            hdu_list_to_write[0] = fits.PrimaryHDU(data=hdu_list_to_write[0].data, header=hdu_list_to_write[0].header)
        fits_utils.convert_extension_datatypes(hdu_list_to_write, context.REDUCED_DATA_EXTENSION_TYPES)
        if context.fpack:
            hdu_list_to_write = fits_utils.pack(hdu_list_to_write, context.LOSSLESS_EXTENSIONS)
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
        if isinstance(other, ObservationFrame):
            return self.primary_hdu - other.primary_hdu
        else:
            return self.primary_hdu - other

    def __isub__(self, other):
        if isinstance(other, ObservationFrame):
            self.primary_hdu.__isub__(other.primary_hdu)
        else:
            self.primary_hdu.__isub__(other)
        return self

    def __mul__(self, other):
        if isinstance(other, ObservationFrame):
            return self.primary_hdu.__mul__(other.primary_hdu)
        else:
            return self.primary_hdu.__mul__(other)

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

    def __contains__(self, key):
        return key in self._hdu_mapping

    def __getitem__(self, item):
        item_index = self._hdu_mapping.get(item)
        if item_index is None:
            return None
        else:
            return self._hdus[item_index]


class CalibrationFrame(metaclass=abc.ABCMeta):
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

    @abc.abstractmethod
    def to_db_record(self, output_product):
        pass

    def write(self, data_products, runtime_context):
        for product in data_products:
            dbs.save_calibration_info(self.to_db_record(product), runtime_context.db_address)


class FrameFactory:
    @property
    @abc.abstractmethod
    def observation_frame_class(self):
        pass

    @property
    @abc.abstractmethod
    def calibration_frame_class(self):
        pass

    @property
    @abc.abstractmethod
    def data_class(self):
        pass

    @property
    @abc.abstractmethod
    def associated_extensions(self):
        pass

    @abc.abstractmethod
    def open(self, file_info, context) -> Optional[ObservationFrame]:
        pass

    @staticmethod
    @abc.abstractmethod
    def get_instrument_from_header(header, db_address):
        pass
