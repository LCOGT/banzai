from pylcogt.stages import Stage
from pylcogt import logs
import os
import numpy as np


class GainNormalizer(Stage):
    def __init__(self, pipeline_context):
        super(GainNormalizer, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for image in images:

            logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
            logs.add_tag(logging_tags, 'filename', os.basename(image.filename))
            logs.add_tag(logging_tags, 'gain', image.gain)
            self.logger.debug('Multiplying by gain', extra=logging_tags)

            if len(image.data.shape) > 2:
                n_amps = image.data.shape[0]
                gain = np.array(eval(image.gain))
                for i in range(n_amps):
                    image.data[i] *= gain[i]
                image.header['SATURATE'] *= min(gain)
                image.header['MAXLIN'] *= min(gain)
            else:
                image.data *= image.gain
                image.header['SATURATE'] *= image.gain
                image.header['MAXLIN'] *= 1.0

            image.gain = 1.0
            image.header['GAIN'] = 1.0
        return images
