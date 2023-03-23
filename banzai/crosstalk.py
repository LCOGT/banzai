import numpy as np

from banzai.stages import Stage
from banzai.logs import get_logger

logger = get_logger()


class CrosstalkCorrector(Stage):
    def __init__(self, runtime_context):
        super(CrosstalkCorrector, self).__init__(runtime_context)

    def do_stage(self, image):
        n_amps = len(image.ccd_hdus)
        if n_amps > 1:
            logging_tags = {}
            crosstalk_matrix = np.identity(n_amps)
            for j in range(n_amps):
                for i in range(n_amps):
                    if i != j:
                        crosstalk_keyword = 'CRSTLK{0}{1}'.format(i + 1, j + 1)
                        crosstalk_matrix[i, j] = -float(image.meta[crosstalk_keyword])
                        logging_tags[crosstalk_keyword] = image.meta[crosstalk_keyword]
            logger.info('Removing crosstalk', image=image, extra_tags=logging_tags)
            # Technically, we should iterate this process because crosstalk doesn't
            # produce more crosstalk
            """This dot product is effectively the following:
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
            corrected_data = np.dot(crosstalk_matrix.T, np.swapaxes([extension.data for extension in image.ccd_hdus], 0, 1))
            for extension, corrected in zip(image.ccd_hdus, corrected_data):
                extension.data = corrected
        return image
