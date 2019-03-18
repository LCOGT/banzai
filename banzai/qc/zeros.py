import logging

import numpy as np

from banzai.stages import Stage
from banzai.utils import qc

logger = logging.getLogger(__name__)


class ZerosTest(Stage):
    """
    Reject any images that have ZEROS_THRESHOLD or more of their pixels exactly equal to 0.

    Notes
    =====
    Sometimes when a camera fails, all pixels have a value of 0.
    """
    ZEROS_THRESHOLD = 0.95

    def __init__(self, pipeline_context):
        super(ZerosTest, self).__init__(pipeline_context)

    def do_stage(self, image):
        npixels = np.product(image.data.shape)
        fraction_0s = float(np.sum(image.data == 0)) / npixels
        logging_tags = {'FRAC0': fraction_0s,
                        'threshold': self.ZEROS_THRESHOLD}
        has_0s_error = fraction_0s > self.ZEROS_THRESHOLD
        qc_results = {'zeros_test.failed': has_0s_error,
                      'zeros_test.fraction': fraction_0s,
                      'zeros_testthreshold': self.ZEROS_THRESHOLD}
        if has_0s_error:
            logger.error('Image is mostly 0s. Rejecting image', image=image, extra_tags=logging_tags)
            qc_results['rejected'] = True
            return None
        else:
            logger.info('Measuring fraction of 0s.', image=image, extra_tags=logging_tags)
        qc.save_qc_results(self.pipeline_context, qc_results, image)

        return image
