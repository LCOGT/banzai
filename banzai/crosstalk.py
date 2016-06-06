from banzai.stages import Stage
from banzai import logs
import numpy as np
import os


class CrosstalkCorrector(Stage):
    def __init__(self, pipeline_context):
        super(CrosstalkCorrector, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for image in images:
            if len(image.data.shape) > 2:
                logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
                logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
                n_amps = image.data.shape[0]
                crosstalk_matrix = np.identity(n_amps)
                for j in range(n_amps):
                    for i in range(n_amps):
                        if i != j:
                            crosstalk_keyword = 'CRSTLK{0}{1}'.format(i + 1, j + 1)
                            crosstalk_matrix[j, i] = -float(image.header[crosstalk_keyword])
                            logs.add_tag(logging_tags, crosstalk_keyword,
                                         image.header[crosstalk_keyword])
                self.logger.info('Removing crosstalk', extra=logging_tags)
                image.data = np.dot(crosstalk_matrix, np.swapaxes(image.data, 0, 1))
        return images
