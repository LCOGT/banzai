from banzai.tests.utils import FakeLCOObservationFrame, FakeCCDData
import numpy as np


def get_dark_pattern(nx, ny, master_dark_fraction):
    size = nx * ny
    n_hot_pixels = int(size*master_dark_fraction)
    xinds = np.random.choice(np.arange(nx), size=n_hot_pixels, replace=True)
    yinds = np.random.choice(np.arange(ny), size=n_hot_pixels, replace=True)

    dark_pattern = np.zeros((ny, nx))
    for x, y in zip(xinds, yinds):
        dark_pattern[y, x] = np.abs(np.random.normal(10, 3))
    return dark_pattern


def make_realistic_master_dark(dark_pattern, nx=101, ny=103, dark_level=30.0,
                               dark_exptime=900.0, read_noise=10.0):

    n_stacked_images = 100
    data = dark_level + dark_pattern * dark_exptime
    dark_noise = np.random.poisson(data) + np.random.normal(0.0, read_noise, size=(ny, nx))
    dark_noise /= np.sqrt(n_stacked_images)
    data += dark_noise
    data /= dark_exptime
    uncertainty = dark_noise * np.ones((ny, nx)) / dark_exptime


    return FakeLCOObservationFrame(hdu_list=[FakeCCDData(data=data, meta={'OBSTYPE': 'DARK',
                                                                          'EXPTIME': dark_exptime},
                                                         uncertainty=uncertainty)])

