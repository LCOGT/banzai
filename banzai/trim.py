import logging
from banzai.stages import Stage

logger = logging.getLogger('banzai')


class Trimmer(Stage):
    def __init__(self, runtime_context):
        super(Trimmer, self).__init__(runtime_context)

    def do_stage(self, image):
        logger.info('Trimming image', image=image)
        # TODO: this enumeration might not actually work, add a replace method in the image class
        for i, data in enumerate(image.ccd_hdus):
            trimmed_data = data.trim()
            image.insert(i, trimmed_data)
            image.remove(data)
        return image
