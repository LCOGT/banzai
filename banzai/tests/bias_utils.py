from banzai.tests.utils import FakeContext, FakeImage
import numpy as np


class FakeBiasImage(FakeImage):
    def __init__(self, context=None, data=None, bias_level=0.0, nx=101, ny=103, header=None):
        super(FakeBiasImage, self).__init__(image_multiplier=bias_level, nx=nx, ny=ny, pipeline_context=context,
                                            data=data, header=header)
        for key, value in {'BIASLVL': bias_level, 'OBSTYPE': 'BIAS'}.items():
            self.header[key] = value


def make_context_with_master_bias(bias_level=0.0, readnoise=10.0, nx=101, ny=103):
    fake_master_bias = FakeBiasImage(bias_level=bias_level, nx=nx, ny=ny)
    fake_master_bias.data = np.random.normal(0.0, readnoise, size=(ny, nx))
    return FakeContext(frame_class=lambda *args, **kwargs: fake_master_bias)
