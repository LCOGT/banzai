from banzai.tests.utils import FakeImage, FakeContext
import numpy as np
from astropy.io import fits


class FakeFlatImage(FakeImage):
    def __init__(self, flat_level=10000.0, data=None, header=None, **kwargs):
        super(FakeFlatImage, self).__init__(**kwargs)
        if header is None:
            self.header = fits.Header({'FLATLVL': flat_level, 'OBSTYPE': 'SKYFLAT', 'TELESCOP': '1m0-10'})
        else:
            self.header = header
        if data is not None:
            self.data = data


def make_context_with_master_flat(flat_level, master_flat_variation=0.05, nx=101, ny=103):
    fake_master_flat = FakeFlatImage(flat_level=flat_level, nx=nx, ny=ny)
    fake_master_flat.data = np.random.normal(1.0, master_flat_variation, size=(ny, nx))
    return FakeContext(frame_class=lambda *args, **kwargs: fake_master_flat)
