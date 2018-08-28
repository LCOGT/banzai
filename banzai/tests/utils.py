from __future__ import absolute_import, division, print_function, unicode_literals
import pytest
from banzai.utils import image_utils
import numpy as np
from datetime import datetime
from banzai.stages import Stage
from banzai.images import Image


class FakeImage(Image):
    def __init__(self, nx=101, ny=103, image_multiplier=1.0,
                 ccdsum='2 2', epoch='20160101', n_amps=1):
        self.nx = nx
        self.ny = ny
        self.telescope_id = -1
        self.site = 'elp'
        self.instrument = 'kb76'
        self.ccdsum = ccdsum
        self.epoch = epoch
        self.data = image_multiplier * np.ones((ny, nx), dtype=np.float32)
        if n_amps > 1:
            self.data = np.stack(n_amps*[self.data])
        self.filename = 'test.fits'
        self.filter = 'U'
        self.dateobs = datetime(2016, 1, 1)
        self.header = {}
        self.caltype = ''
        self.bpm = np.zeros((ny, nx), dtype=np.uint8)
        self.request_number = '0000331403'
        self.readnoise = 11.0
        self.block_id = '254478983'
        self.molecule_id = '544562351'
        self.exptime = 30.0
        self.obstype = 'TEST'

    def get_calibration_filename(self):
        return '/tmp/{0}_{1}_{2}_bin{3}.fits'.format(self.caltype, self.instrument,
                                                     self.epoch,
                                                     self.ccdsum.replace(' ', 'x'))

    def subtract(self, x):
        self.data -= x

    def add_history(self, msg):
        pass


class FakeContext(object):
    def __init__(self, preview_mode=False):
        self.processed_path = '/tmp'
        self.preview_mode = preview_mode


class FakeStage(Stage):
    def do_stage(self, images):
        return images
    def group_by_keywords(self):
        return None


def throws_inhomogeneous_set_exception(stagetype, context, keyword, value):
    stage = stagetype(context)

    with pytest.raises(image_utils.InhomogeneousSetException) as exception_info:
        kwargs = {keyword: value}
        images = [FakeImage(**kwargs)]
        images += [FakeImage() for x in range(6)]
        stage.do_stage(images)
    assert 'Images have different {0}s'.format(keyword) == str(exception_info.value)


def gaussian2d(image_shape, x0, y0, brightness, fwhm):
    x = np.arange(image_shape[1])
    y = np.arange(image_shape[0])
    x2d, y2d = np.meshgrid(x, y)

    sig = fwhm  / 2.35482

    normfactor = brightness / 2.0 / np.pi * sig ** -2.0
    exponent = -0.5 * sig ** -2.0
    exponent *= (x2d - x0) ** 2.0 + (y2d - y0) ** 2.0

    return normfactor * np.exp(exponent)
