from banzai.tests.utils import FakeContext, FakeImage
import numpy as np
from astropy.io import fits


class FakeBiasImage(FakeImage):
    def __init__(self, context=None, data=None, bias_level=0.0, nx=101, ny=103, header=None):
        super(FakeBiasImage, self).__init__(image_multiplier=bias_level, nx=nx, ny=ny)
        if data is not None:
            self.data = data
        if header is None:
            self.header = fits.Header({'BIASLVL': bias_level, 'OBSTYPE': 'BIAS', 'TELESCOP': '1m0-10'})
        else:
            self.header = header


def make_context_with_master_bias(bias_level, readnoise, nx, ny):
    fake_master_bias = FakeBiasImage(bias_level=bias_level, nx=nx, ny=ny)
    fake_master_bias.data = np.random.normal(0.0, readnoise, size=(ny, nx))

    context = FakeContext()
    context.FRAME_CLASS = lambda *args, **kwargs: fake_master_bias
    return context
