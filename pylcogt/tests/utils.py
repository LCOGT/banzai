import pytest
from ..images import InhomogeneousSetException
import numpy as np
from datetime import datetime


class FakeImage(object):
    def __init__(self, nx=101, ny=103, image_multiplier=1.0,
                 ccdsum='2 2', epoch='20160101',):
        self.nx = nx
        self.ny = ny
        self.telescope_id = -1
        self.site = 'elp'
        self.instrument = 'kb76'
        self.ccdsum = ccdsum
        self.epoch = epoch
        self.data = image_multiplier * np.ones((ny, nx))
        self.filename = 'test.fits'
        self.filter = 'U'
        self.dateobs = datetime(2016, 1, 1)
        self.header = {}
        self.caltype = ''

    def get_calibration_filename(self):
        return '/tmp/{0}_{1}_{2}_bin{3}.fits'.format(self.caltype, self.instrument,
                                                     self.epoch,
                                                     self.ccdsum.replace(' ', 'x'))

    def subtract(self, x):
        self.data -= x

    def add_history(self, msg):
        pass


class FakeContext(object):
    def __init__(self):
        self.processed_path = '/tmp'


def throws_inhomogeneous_set_exception(stagetype, context, keyword, value):
    stage = stagetype(context)

    with pytest.raises(InhomogeneousSetException) as exception_info:
        kwargs = {keyword: value}
        images = [FakeImage(**kwargs)]
        images += [FakeImage() for x in range(6)]
        stage.do_stage(images)
    assert 'Images have different {0}s'.format(keyword) == str(exception_info.value)
