import logging

from banzai.stages import Stage

logger = logging.getLogger('banzai')


class GainNormalizer(Stage):
    def __init__(self, runtime_context):
        super(GainNormalizer, self).__init__(runtime_context)

    def do_stage(self, image):
        logger.info('Multiplying by gain', image=image, extra_tags={'gain': image.gain})
        for data in image.image_extensions:
            data *= data.gain
        return image
