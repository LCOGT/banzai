import json
from datetime import datetime, timedelta

import numpy as np
from astropy.io.fits import Header

from banzai.stages import Stage
from banzai.images import Image
from banzai.utils.date_utils import TIMESTAMP_FORMAT
import logging
logger = logging.getLogger('banzai')


class FakeImage(Image):
    def __init__(self, runtime_context=None, nx=101, ny=103, image_multiplier=1.0, site='elp', camera='kb76',
                 ccdsum='2 2', epoch='20160101', n_amps=1, filter='U', data=None, header=None, **kwargs):
        self.nx = nx
        self.ny = ny
        self.instrument_id = -1
        self.site = site
        self.camera = camera
        self.ccdsum = ccdsum
        self.epoch = epoch
        if data is None:
            self.data = image_multiplier * np.ones((ny, nx), dtype=np.float32)
        else:
            self.data = data
        if n_amps > 1:
            self.data = np.stack(n_amps*[self.data])
        self.filename = 'test.fits'
        self.filter = filter
        self.dateobs = datetime(2016, 1, 1)
        if header is None:
            header = Header({'TELESCOP': '1m0-10'})
        self.header = header
        self.caltype = ''
        self.bpm = np.zeros((ny, nx), dtype=np.uint8)
        self.request_number = '0000331403'
        self.readnoise = 11.0
        self.block_id = '254478983'
        self.molecule_id = '544562351'
        self.exptime = 30.0
        self.obstype = 'TEST'
        self.is_bad = False
        self.configuration_mode = 'full_frame'
        for keyword in kwargs:
            setattr(self, keyword, kwargs[keyword])

    def get_calibration_filename(self):
        return '/tmp/{0}_{1}_{2}_bin{3}.fits'.format(self.caltype, self.instrument,
                                                     self.epoch,
                                                     self.ccdsum.replace(' ', 'x'))

    def subtract(self, x):
        self.data -= x

    def add_history(self, msg):
        pass

    def get_n_amps(self):
        if len(self.data.shape) > 2:
            n_amps = self.data.shape[0]
        else:
            n_amps = 1
        return n_amps


class FakeContext(object):
    def __init__(self, preview_mode=False, frame_class=FakeImage):
        self.FRAME_CLASS = frame_class
        self.preview_mode = preview_mode
        self.processed_path = '/tmp'
        self.db_address = 'sqlite:foo'
        self.ignore_schedulability = False
        self.max_tries = 5

    def image_can_be_processed(self, header):
        return True


class FakeStage(Stage):
    def do_stage(self, images):
        return images


def handles_inhomogeneous_set(stagetype, context, keyword, value, calibration_maker=False):
    logger.error(vars(context))
    stage = stagetype(context)
    kwargs = {keyword: value}
    if calibration_maker:
        images = [FakeImage(**kwargs)]
        images += [FakeImage() for x in range(6)]
        images = stage.do_stage(images)
        assert len(images) == 0
    else:
        image = FakeImage(**kwargs)
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
    midnight_at_site = datetime.strptime(dayobs, '%Y%m%d') + timedelta(hours=24-timezone)
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
