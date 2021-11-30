import logging
from banzai.stages import Stage
from cosmic_conn.inference_cr import init_model

logger = logging.getLogger('banzai')


class CosmicRayDetector(Stage):
    def __init__(self, runtime_context):
        super(CosmicRayDetector, self).__init__(runtime_context)

    def do_stage(self, image):

        # initialize a Cosmic-CoNN model
        cr_model, opt = init_model("ground_imaging")

        # the model outputs a CR probability map in np.float32
        cr_prob = cr_model.detect_cr(image.data)

        # convert the probability map to a boolean mask with a 0.5 threshold
        # This value produces a 5% false discovery rate with 94% completeness.
        cr_mask = cr_prob > 0.5

        image.mask[cr_mask] |= 4
        return image
