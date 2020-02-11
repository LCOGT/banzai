from banzai.tests.utils import FakeImage, FakeContext, FakeLCOObservationFrame, FakeCCDData
import numpy as np


class FakeFlatImage(FakeImage):
    def __init__(self, runtime_context=None, flat_level=10000.0, data=None, header=None, **kwargs):
        super(FakeFlatImage, self).__init__(runtime_context=runtime_context, data=data, header=header, **kwargs)
        for key, value in {'FLATLVL': flat_level, 'OBSTYPE': 'SKYFLAT'}.items():
            self.header[key] = value


def make_realistic_master_flat(flat_level=1.0, master_flat_variation=0.05, read_noise=11.0, nx=101, ny=103):
    fake_master_flat = FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=np.random.normal(flat_level, master_flat_variation, size=(ny, nx)),
                                                                     meta={'OBSTYPE': "SKYFLAT",
                                                                           'FLATLVL': flat_level},
                                                                     read_noise=read_noise)])
    if flat_level != 1.0:
        fake_master_flat.primary_hdu.data /= flat_level
        fake_master_flat.primary_hdu.uncertainty /= flat_level

    return fake_master_flat
