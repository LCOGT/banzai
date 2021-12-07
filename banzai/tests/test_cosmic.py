import numpy as np
from banzai.tests.utils import gaussian2d
from banzai.cosmic import CosmicRayDetector
from banzai.tests.utils import FakeContext, FakeCCDData, FakeLCOObservationFrame


def test_cosmic_detection_is_reasonable():
    input_readnoise = 9.0
    nx = 1001
    ny = 1003

    # Set a seed so that the tests are repeatable
    np.random.seed(891273492)

    # Create a simulated image to use in our tests
    imdata = np.zeros((1001, 1001), dtype=np.float32)

    # Add sky and sky noise
    imdata += 200

    # Add some fake sources
    for i in range(100):
        x = np.random.uniform(low=0.0, high=1001)
        y = np.random.uniform(low=0.0, high=1001)
        brightness = np.random.uniform(low=1000., high=30000.)
        imdata += gaussian2d(imdata.shape, x, y, brightness, 3.5)

    # Add the poisson noise
    imdata = np.random.poisson(imdata).astype(float)

    # Add readnoise
    imdata += np.random.normal(0.0, input_readnoise, size=(1001, 1001))

    # Add 100 fake cosmic rays
    cr_x = np.random.randint(low=5, high=995, size=100)
    cr_y = np.random.randint(low=5, high=995, size=100)

    cr_brightnesses = np.random.uniform(low=1000.0, high=30000.0, size=100)

    imdata[cr_y, cr_x] += cr_brightnesses

    # Make a mask where the detected cosmic rays should be
    crmask = np.zeros((1001, 1001), dtype=bool)
    crmask[cr_y, cr_x] = True

    cosmic_stage = CosmicRayDetector(FakeContext())
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(nx=nx, ny=ny, read_noise=input_readnoise, data=imdata,
                                                          uncertainty=np.sqrt(input_readnoise**2.0 + imdata))])
    image = cosmic_stage.do_stage(image)

    actual_detections = (image.mask & 4) == 4
    assert (crmask == actual_detections).sum() == crmask.size
