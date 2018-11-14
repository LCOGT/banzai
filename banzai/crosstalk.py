import logging

import numpy as np

from banzai.stages import Stage

logger = logging.getLogger(__name__)


class CrosstalkCorrector(Stage):
    def __init__(self, pipeline_context):
        super(CrosstalkCorrector, self).__init__(pipeline_context)

    def do_stage(self, images):
        for image in images:
            if image.data_is_3d():
                logging_tags = {}
                n_amps = image.get_n_amps()
                crosstalk_matrix = np.identity(n_amps)
                for j in range(n_amps):
                    for i in range(n_amps):
                        if i != j:
                            crosstalk_keyword = 'CRSTLK{0}{1}'.format(i + 1, j + 1)
                            crosstalk_matrix[i, j] = -float(image.header[crosstalk_keyword])
                            logging_tags[crosstalk_keyword] = image.header[crosstalk_keyword]
                logger.info('Removing crosstalk', image=image, extra_tags=logging_tags)
                # Techinally, we should iterate this process because crosstalk doesn't
                # produce more crosstalk
                """This dot product is effectivly the following:
                coeffs = [[Q11, Q12, Q13, Q14],
                          [Q21, Q22, Q23, Q24],
                          [Q31, Q32, Q33, Q34],
                          [Q41, Q42, Q43, Q44]]

                The corrected data, D, from quadrant i is
                D1 = D1 - Q21 D2 - Q31 D3 - Q41 D4
                D2 = D2 - Q12 D1 - Q32 D3 - Q42 D4
                D3 = D3 - Q13 D1 - Q23 D2 - Q43 D4
                D4 = D4 - Q14 D1 - Q24 D2 - Q34 D3
                """
                image.data = np.dot(crosstalk_matrix.T, np.swapaxes(image.data, 0, 1))
        return images
