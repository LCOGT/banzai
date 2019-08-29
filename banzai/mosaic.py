import logging
import numpy as np
from banzai.stages import Stage

logger = logging.getLogger('banzai')


class MosaicCreator(Stage):
    def __init__(self, runtime_context):
        super(MosaicCreator, self).__init__(runtime_context)

    def do_stage(self, image):
        logger.info('Mosaicing image', image=image)
        nx, ny = image.get_mosaic_size()
        mosaiced_data = CCDData(data=np.zeros(ny, nx), mask=np.ones(ny, nx, dtype=np.uint8))
        for data in image.image_extensions:
            data.copy_to(mosaiced_data)
            image.image_extensions.pop(data)
        image.primary_hdu = mosaiced_data
        return image
