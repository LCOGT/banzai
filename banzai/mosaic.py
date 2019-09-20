import logging
import numpy as np
from banzai.stages import Stage
from banzai.images import CCDData

logger = logging.getLogger('banzai')


class MosaicCreator(Stage):
    def __init__(self, runtime_context):
        super(MosaicCreator, self).__init__(runtime_context)

    def do_stage(self, image):
        logger.info('Mosaicing image', image=image)
        nx, ny = image.get_mosaic_size()
        mosaiced_data = CCDData(data=np.zeros(ny, nx), meta=image.primary_hdu.meta)
        mosaiced_data.binning = image.binning
        for data in image.image_extensions:
            data.copy_to_mosaic(mosaiced_data)
            image.remove(data)
        image.primary_hdu = mosaiced_data
        return image
