import json
from datetime import datetime, timedelta
from types import ModuleType

import numpy as np
from astropy.io.fits import Header

from banzai import settings
from banzai.stages import Stage
from banzai.lco import LCOObservationFrame, LCOCalibrationFrame
from banzai.utils.image_utils import Section
from banzai.data import HeaderOnly, CCDData
from banzai.utils.date_utils import TIMESTAMP_FORMAT
import logging

logger = logging.getLogger('banzai')


class FakeCCDData(CCDData):
    def __init__(self, image_multiplier=1.0, nx=101, ny=103, name='test_image', read_noise=None,
                 bias_level=None, meta=None, data=None, mask=None, uncertainty=None, **kwargs):
        self.name = name
        if meta is not None:
            self.meta = meta
        else:
            self.meta = Header()
        if bias_level is not None:
            self.meta['BIASLVL'] = bias_level
        if read_noise is not None:
            self.meta['RDNOISE'] = read_noise
        self._detector_section = Section.parse_region_keyword(self.meta.get('DETSEC'))
        self._data_section = Section.parse_region_keyword(self.meta.get('DATASEC'))

        if data is None:
            self.data = image_multiplier * np.ones((ny, nx), dtype=np.float32)
        else:
            self.data = data
        if mask is None:
            self.mask = np.zeros(self.data.shape, dtype=np.uint8)
        else:
            self.mask = mask
        if uncertainty is None:
            self.uncertainty = self.read_noise * np.ones(self.data.shape, dtype=self.data.dtype)
        else:
            self.uncertainty = uncertainty

        for keyword in kwargs:
            setattr(self, keyword, kwargs[keyword])


class FakeLCOObservationFrame(LCOObservationFrame):
    def __init__(self, hdu_list=None, file_path='/tmp/test_image.fits', instrument=None, epoch='20160101',
                 **kwargs):
        if hdu_list is None:
            self._hdus = [FakeCCDData()]
        else:
            self._hdus = hdu_list
        if instrument is None:
            self.instrument = FakeInstrument(0, 'cpt', 'fa16', 'doma', '1m0a', '1M-SCICAM-SINISTRO', schedulable=True)
        else:
            self.instrument = instrument
        self.primary_hdu.meta['DAY-OBS'] = epoch
        self._file_path = file_path
        self.is_bad = False
        self.hdu_order = ['SCI', 'CAT', 'BPM', 'ERR']

        for keyword in kwargs:
            setattr(self, keyword, kwargs[keyword])


class FakeContext(object):
    def __init__(self, preview_mode=False, fpack=True, frame_class=FakeLCOObservationFrame, **kwargs):
        self.FRAME_CLASS = frame_class
        self.preview_mode = preview_mode
        self.processed_path = '/tmp'
        self.db_address = 'sqlite:///test.db'
        self.opensearch_qc_index = 'banzai_qc'
        self.ignore_schedulability = False
        self.max_tries = 5
        self.fpack = fpack
        self.reduction_level = '91'
        self.use_only_older_calibrations = False
        # Get all of the settings that are not builtins and store them in the context object
        for setting in dir(settings):
            if '__' != setting[:2] and not isinstance(getattr(settings, setting), ModuleType):
                setattr(self, setting, getattr(settings, setting))

        for keyword in kwargs:
            setattr(self, keyword, kwargs[keyword])

    def image_can_be_processed(self):
        return True


class FakeStage(Stage):
    def do_stage(self, images):
        return images


def handles_inhomogeneous_set(stagetype, context, keyword, value, calibration_maker=False):
    logger.error(vars(context))
    stage = stagetype(context)
    kwargs = {keyword: value}
    if calibration_maker:
        images = [LCOCalibrationFrame(hdu_list=[HeaderOnly(meta=kwargs)])]
        images += [LCOCalibrationFrame(hdu_list=[HeaderOnly()]) for x in range(6)]
        images = stage.do_stage(images)
        assert len(images) == 0
    else:
        image = LCOCalibrationFrame(hdu_list=[CCDData(data=np.zeros(0), meta=kwargs)], file_path='test.fits')
        image = stage.do_stage(image)
        assert image is None


def gaussian2d(image_shape, x0, y0, brightness, fwhm):
    x = np.arange(image_shape[1])
    y = np.arange(image_shape[0])
    x2d, y2d = np.meshgrid(x, y)

    sig = fwhm / 2.35482

    normfactor = brightness / 2.0 / np.pi * sig ** -2.0
    exponent = -0.5 * sig ** -2.0
    exponent *= (x2d - x0) ** 2.0 + (y2d - y0) ** 2.0

    return normfactor * np.exp(exponent)


def get_min_and_max_dates(timezone, dayobs):
    # Gets next midnight relative to date of observation
    midnight_at_site = datetime.strptime(dayobs, '%Y%m%d') + timedelta(hours=24 - timezone)
    min_date = midnight_at_site - timedelta(days=0.5)
    max_date = midnight_at_site + timedelta(days=0.5)
    return min_date.strftime(TIMESTAMP_FORMAT), max_date.strftime(TIMESTAMP_FORMAT)


class FakeResponse(object):
    def __init__(self, filename):
        with open(filename) as f:
            self.data = json.load(f)

    def json(self):
        return self.data

    def raise_for_status(self):
        pass


class FakeInstrument(object):
    def __init__(self, id=0, site='', camera='', enclosure='', telescope='', type='', schedulable=True):
        self.id = id
        self.site = site
        self.camera = camera
        self.enclosure = enclosure
        self.telescope = telescope
        self.schedulable = schedulable
        self.type = type
        self.name = camera


class FakeCalImage:
    def __init__(self):
        self.frameid = 1234
        self.filepath = '/tmp/'
        self.filename = 'test.fits'
