import json
from datetime import datetime
import inspect

import numpy as np
from astropy.io.fits import Header
from astropy.utils.data import get_pkg_data_filename

from banzai.stages import Stage
from banzai.images import Image
import banzai.settings


class FakeImage(Image):
    def __init__(self, pipeline_context=None, nx=101, ny=103, image_multiplier=1.0, site='elp', camera='kb76',
                 ccdsum='2 2', epoch='20160101', n_amps=1, filter='U', data=None, header=None, **kwargs):
        self.nx = nx
        self.ny = ny
        self.instrument_id = -1
        self.site = site
        self.camera = camera
        self.ccdsum = ccdsum
        self.epoch = epoch
        if data is None:
            data = image_multiplier * np.ones((ny, nx), dtype=np.float32)
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

    def get_calibration_filename(self):
        return '/tmp/{0}_{1}_{2}_bin{3}.fits'.format(self.caltype, self.instrument,
                                                     self.epoch,
                                                     self.ccdsum.replace(' ', 'x'))

    def subtract(self, x):
        self.data -= x

    def add_history(self, msg):
        pass


class FakeContext(object):
    def __init__(self, preview_mode=False, settings=banzai.settings.ImagingSettings(), frame_class=FakeImage):
        for key, value in dict(inspect.getmembers(settings)).items():
            if not key.startswith('_'):
                setattr(self, key, value)
        self.FRAME_CLASS = frame_class
        self.preview_mode = preview_mode
        self.processed_path = '/tmp'

    def image_can_be_processed(self, header):
        return True


class FakeStage(Stage):
    def do_stage(self, images):
        return images


def handles_inhomogeneous_set(stagetype, context, keyword, value, calibration_maker=False):
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


class FakeResponse(object):
    def __init__(self):
        with open(get_pkg_data_filename('data/configdb_example.json')) as f:
            self.data = json.load(f)

    def json(self):
        return self.data
