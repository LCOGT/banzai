from banzai.tests.utils import FakeImage, FakeContext
import numpy as np


class FakeFlatImage(FakeImage):
    def __init__(self, pipeline_context=None, flat_level=10000.0, data=None, header=None, **kwargs):
        super(FakeFlatImage, self).__init__(pipeline_context=pipeline_context, data=data, header=header, **kwargs)
        for key, value in {'FLATLVL': flat_level, 'OBSTYPE': 'SKYFLAT'}.items():
            self.header[key] = value


def make_context_with_master_flat(flat_level=1.0, master_flat_variation=0.05, nx=101, ny=103):
    fake_master_flat = FakeFlatImage(data=np.random.normal(flat_level, master_flat_variation, size=(ny, nx)),
                                     nx=nx, ny=ny)
    return FakeContext(frame_class=lambda *args, **kwargs: fake_master_flat)
