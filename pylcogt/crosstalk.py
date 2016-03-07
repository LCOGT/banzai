from pylcogt.stages import Stage
import numpy as np


class CrosstalkCorrector(Stage):

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for image in images:
            if len(image.data.shape) > 2:
                n_amps = image.data.shape[0]
                crosstalk_matrix = np.identity(n_amps)
                for j in range(n_amps):
                    for i in range(n_amps):
                        if i != j:
                            crosstalk_keyword = 'CRSTLK{0}{1}'.format(i + 1, j + 1)
                            crosstalk_matrix[j, i] = -float(image.header[crosstalk_keyword])
                image.data = np.dot(crosstalk_matrix, np.swapaxes(image.data, 0, 1))
