import numpy as np
from skimage.draw import line
from banzai.tests.utils import gaussian2d
from banzai.cosmic import CosmicRayDetector
from banzai.tests.utils import FakeContext, FakeCCDData, FakeLCOObservationFrame

# temp
import imageio


def test_cosmic_detection_is_reasonable():
    input_readnoise = 9.0
    nx = 1001
    ny = 1003

    # Set a seed so that the tests are repeatable
    np.random.seed(891273492)

    # Create a simulated image to use in our tests
    imdata = np.zeros((1001, 1001), dtype=np.float32)

    # Make a mask where the detected cosmic rays should be
    crmask = np.zeros((1001, 1001), dtype=bool)

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

    imageio.imwrite('1_sources.jpg', imdata)

    # Add 100 fake cosmic rays (lines/rectangles for cosmic-conn)
    cr_x = np.random.randint(low=20, high=980, size=100)
    cr_y = np.random.randint(low=20, high=980, size=100)
    
    # shifts in x and y defines the length of the line
    cr_x_len = np.random.randint(low=3, high=20, size=100)
    cr_y_len = np.random.randint(low=3, high=20, size=100)
    
    cr_val = np.random.uniform(low=1000.0, high=30000.0, size=100)

    sign = lambda x: 1 if np.random.random() > 0.5 else -1

    for i in range(len(cr_x)):

        start = (cr_x[i], cr_y[i])
        end = (start[0] + cr_x_len[i]*sign(0), start[1] + cr_y_len[i]*sign(0))

        yy, xx = line(start[0], start[1], end[0], end[1])

        imdata[yy, xx] += cr_val[i]
        crmask[yy, xx] = True

    imageio.imwrite('0_with_lines.jpg', imdata)

    cosmic_stage = CosmicRayDetector(FakeContext())
    image = FakeLCOObservationFrame(hdu_list=[FakeCCDData(nx=nx, ny=ny, read_noise=input_readnoise, data=imdata,
                                                          uncertainty=np.sqrt(input_readnoise**2.0 + imdata))])
    image = cosmic_stage.do_stage(image)

    actual_detections = (image.mask & 4) == 4

    # temp for debugging

    imageio.imwrite('2_actual_detections.jpg', actual_detections* 1)

    TP = (crmask == 1) & (actual_detections == 1)
    FP = (crmask == 0) & (actual_detections == 1)
    FN = (crmask == 1) & (actual_detections == 0)
    P = TP | FN

    breakpoint()

    print(f'{np.sum(TP)/np.sum(P)} TPR/Completeness')
    print(f'{np.sum(FP)/(np.sum(TP) + np.sum(FP))} False Discovery Rate')
    print(f'{(crmask == actual_detections).sum() / crmask.size} True Detections / image size')

    breakpoint()

    assert (crmask == actual_detections).sum() == crmask.size
