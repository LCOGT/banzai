import logging

import numpy as np

from banzai.stages import Stage
from banzai.utils import qc

logger = logging.getLogger(__name__)


class ZerosTest(Stage):
    """
    Reject any images that have ZEROS_THRESHOLD or more of their pixels in a single amp exactly equal to 0.

    Notes
    =====
    Sometimes when a camera fails, all pixels have a value of 0.
    """
    ZEROS_THRESHOLD = 0.95

    def __init__(self, runtime_context):
        super(ZerosTest, self).__init__(runtime_context)

    def do_stage(self, image):
        fraction_0s_list = []
        for i_amp in range(image.get_n_amps()):
            npixels = np.product(image.data[i_amp].shape)
            fraction_0s_list.append(float(np.sum(image.data[i_amp] == 0)) / npixels)

        has_0s_error = any([fraction_0s > self.ZEROS_THRESHOLD for fraction_0s in fraction_0s_list])
        logging_tags = {'FRAC0': fraction_0s_list,
                        'threshold': self.ZEROS_THRESHOLD}
        qc_results = {'zeros_test.failed': has_0s_error,
                      'zeros_test.fraction': fraction_0s_list,
                      'zeros_test.threshold': self.ZEROS_THRESHOLD}
        if has_0s_error:
            logger.error('Image is mostly 0s. Rejecting image', image=image, extra_tags=logging_tags)
            qc_results['rejected'] = True
            return None
        else:
            logger.info('Measuring fraction of 0s.', image=image, extra_tags=logging_tags)
        qc.save_qc_results(self.runtime_context, qc_results, image)

        return image
