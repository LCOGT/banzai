import logging
from banzai.stages import Stage

logger = logging.getLogger('banzai')


class Trimmer(Stage):
    def __init__(self, runtime_context):
        super(Trimmer, self).__init__(runtime_context)

    def do_stage(self, image):
        logger.info('Trimming image', image=image)
        for data in image.ccd_hdus():
            data.trim()
        return image
