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
from banzai.utils import stats
from banzai.logs import get_logger

logger = get_logger()


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
        self.memmap = True
        if uncertainty is None:
            self.uncertainty = self.read_noise * np.ones(self.data.shape, dtype=self.data.dtype)
        else:
            self.uncertainty = uncertainty
        if 'SATURATE' not in self.meta:
            self.meta['SATURATE'] = 65535.0
        if 'GAIN' not in self.meta:
            self.meta['GAIN'] = 1.0
        if 'MAXLIN' not in self.meta:
            self.meta['MAXLIN'] = 65535.0
        for keyword in kwargs:
            setattr(self, keyword, kwargs[keyword])


def current_stack_snapshot(data_to_stack, nsigma_reject):
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
        self.n_sub_exposures = 1
        self.frame_id = 12
        for keyword in kwargs:
            setattr(self, keyword, kwargs[keyword])


class FakeContext(object):
    def __init__(self, preview_mode=False, fpack=True, frame_class=FakeLCOObservationFrame, **kwargs):
        self.FRAME_CLASS = frame_class
        self.preview_mode = preview_mode
        self.processed_path = '/tmp'
        self.db_address = 'sqlite:///test.db'
        self.cal_db_address = self.db_address
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
        images = [LCOCalibrationFrame(hdu_list=[HeaderOnly(meta=kwargs, name='')])]
        images += [LCOCalibrationFrame(hdu_list=[HeaderOnly(meta={}, name=''),]) for x in range(6)]
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
        self.nx = 4096
        self.ny = 4096


class FakeCalImage:
    def __init__(self):
        self.frameid = 1234
        self.filepath = '/tmp/'
        self.filename = 'test.fits'
