import numpy as np
from skimage.draw import line
from banzai.tests.utils import gaussian2d
from banzai.cosmic import CosmicRayDetector
from banzai.tests.utils import FakeContext, FakeCCDData, FakeLCOObservationFrame


def generate_test_image(input_readnoise, n_source, n_cr):
    # Create a simulated image to use in our tests
    imdata = np.zeros((1001, 1001), dtype=np.float32)

    # Make a mask where the detected cosmic rays should be
    crmask = np.zeros((1001, 1001), dtype=bool)

    # Add sky and sky noise
    imdata += 200

    # Add some fake sources
    for i in range(n_source):
        x = np.random.uniform(low=0.0, high=1001)
        y = np.random.uniform(low=0.0, high=1001)
        brightness = np.random.uniform(low=1000., high=30000.)
        imdata += gaussian2d(imdata.shape, x, y, brightness, 3.5)

    # Add the poisson noise
    imdata = np.random.poisson(imdata).astype(float)

    # Add readnoise
    imdata += np.random.normal(0.0, input_readnoise, size=(1001, 1001))

    # Add 100 fake cosmic rays (lines/rectangles for cosmic-conn)
    cr_x = np.random.randint(low=20, high=980, size=n_cr)
    cr_y = np.random.randint(low=20, high=980, size=n_cr)
    
    # shifts in x and y defines the length of the line
    cr_x_len = np.random.randint(low=3, high=20, size=n_cr)
    cr_y_len = np.random.randint(low=3, high=20, size=n_cr)
    
    cr_val = np.random.uniform(low=1000.0, high=30000.0, size=n_cr)

    sign = lambda x: 1 if np.random.random() > 0.5 else -1

    for i in range(n_cr):

        start = (cr_x[i], cr_y[i])
        end = (cr_x[i] + cr_x_len[i]*sign(0), cr_y[i] + cr_y_len[i]*sign(0))

        yy, xx = line(start[0], start[1], end[0], end[1])

        imdata[yy, xx] += cr_val[i]
        crmask[yy, xx] = True

    return imdata, crmask


def one_img_cosmic_detection(cosmic_stage, nx, ny, imdata, crmask, input_readnoise):

    image = FakeLCOObservationFrame(
        hdu_list=[FakeCCDData(nx=nx, ny=ny, 
            read_noise=input_readnoise, 
            data=imdata, 
            uncertainty=np.sqrt(input_readnoise**2.0 + imdata))]
            )

    image = cosmic_stage.do_stage(image)

    actual_detections = (image.mask & 4) == 4

    TP = (crmask == 1) & (actual_detections == 1)
    FP = (crmask == 0) & (actual_detections == 1)
    FN = (crmask == 1) & (actual_detections == 0)
    P = TP | FN

    completeness = np.sum(TP) / np.sum(P)
    false_discovery_rate = np.sum(FP) / (np.sum(TP) + np.sum(FP))
    accuracy = (crmask == actual_detections).sum() / crmask.size

    return completeness, false_discovery_rate, accuracy


def test_cosmic_detection_is_reasonable():
    nx = 1001
    ny = 1003
    input_readnoise = 9.0
    n_source = 100
    n_cr = 100

    # Set a seed so that the tests are repeatable
    np.random.seed(891273492)

    cosmic_stage = CosmicRayDetector(FakeContext())

    # test ten random images
    N = 10
    completeness, false_discovery_rate, accuracy = [], [], []

    for i in range(N):
        imdata, crmask = generate_test_image(input_readnoise, n_source, n_cr)
        c, f, a = one_img_cosmic_detection(cosmic_stage, nx, ny, imdata, crmask, input_readnoise)

        completeness.append(c)
        false_discovery_rate.append(f)
        accuracy.append(a)

    # Full performance evaluation, please see Cosmic-CoNN paper:
    # https://arxiv.org/abs/2106.14922

    # banzai uses a default 0.5 threshold to convert the probability map to a boolean mask
    # this value produces a 5% false discovery rate with 94% completeness on real data
    # the test shall pass if the averge performance on 10 fakes images are equal or better
    assert np.mean(completeness) >= 0.94
    assert np.mean(false_discovery_rate) <= 0.05
