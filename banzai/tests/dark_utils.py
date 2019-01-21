from banzai.tests.utils import FakeImage, FakeContext
import numpy as np


class FakeDarkImage(FakeImage):

    def __init__(self, pipeline_context=None, exptime=30.0, filename='', data=None, header=None, **kwargs):
        self.exptime = exptime
        super(FakeDarkImage, self).__init__(pipeline_context=pipeline_context, data=data, header=header, **kwargs)
        self.header['OBSTYPE'] = 'DARK'


def get_dark_pattern(nx, ny, master_dark_fraction):
    size = nx * ny
    n_hot_pixels = int(size*master_dark_fraction)
    xinds = np.random.choice(np.arange(nx), size=n_hot_pixels, replace=True)
    yinds = np.random.choice(np.arange(ny), size=n_hot_pixels, replace=True)

    dark_pattern = np.zeros((ny, nx))
    for x, y in zip(xinds, yinds):
        dark_pattern[y, x] = np.abs(np.random.normal(10, 3))
    return dark_pattern


def make_context_with_realistic_master_dark(dark_pattern, nx=101, ny=103, dark_level=30.0, dark_exptime=900.0):
    fake_master_dark = FakeDarkImage(dark_level)
    fake_master_dark.data = np.random.normal(0.0, fake_master_dark.readnoise / 10.0, size=(ny, nx)) / dark_exptime

    master_dark_noise = np.random.poisson(dark_pattern * dark_exptime) - dark_pattern * dark_exptime
    fake_master_dark.data += dark_pattern + master_dark_noise / dark_exptime / 10.0

    context = FakeContext(frame_class=lambda *args, **kwargs: fake_master_dark)
    context.dark_pattern = dark_pattern
    return context
