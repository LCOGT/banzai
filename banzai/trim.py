from banzai.logs import get_logger
from banzai.stages import Stage

logger = get_logger()


class Trimmer(Stage):
    def __init__(self, runtime_context):
        super(Trimmer, self).__init__(runtime_context)

    def do_stage(self, image):
        logger.info('Trimming image', image=image)
        # TODO: this enumeration might not actually work, add a replace method in the image class
        data_to_replace = []
        for i, data in enumerate(image.ccd_hdus):
            trimmed_data = data.trim()
            data_to_replace.append((data, trimmed_data))

        for old_data, trimmed_data in data_to_replace:
            image.replace(old_data, trimmed_data)
        return image
