from __future__ import absolute_import, division, print_function, unicode_literals
from banzai.stages import Stage
from banzai import logs
import os
import numpy as np


class GainNormalizer(Stage):
    def __init__(self, pipeline_context):
        super(GainNormalizer, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        images_to_remove = []
        for image in images:

            logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            logs.add_tag(logging_tags, 'gain', image.gain)
            self.logger.info('Multiplying by gain', extra=logging_tags)

            gain = image.gain
            if gain_missing(gain):
                self.logger.error('Gain missing. Rejecting image.', extra=logging_tags)
                images_to_remove.append(image)
            else:
                if len(image.data.shape) > 2:
                    n_amps = image.data.shape[0]
                    for i in range(n_amps):
                        image.data[i] *= gain[i]
                    image.header['SATURATE'] *= min(gain)
                    image.header['MAXLIN'] *= min(gain)
                else:
                    image.data *= image.gain
                    image.header['SATURATE'] *= image.gain
                    image.header['MAXLIN'] *= image.gain

                image.gain = 1.0
                image.header['GAIN'] = 1.0

        for image in images_to_remove:
            images.remove(image)

        return images


def gain_missing(gain):
    # Catch empty list, gain = 0, and gain is None
    if gain:
        # Make sure there aren't any zeros in the list of gains
        try:
            missing = not all(gain)
        # Catch the case when gain is a float (gain = zero and gain = None cases are tested above).
        except TypeError:
            missing = False
    else:
        missing = True
    return missing
