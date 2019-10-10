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
        mosaiced_detector_region = image.get_mosaic_detector_region()
        binned_shape = [length // binning for length, binning in zip(mosaiced_detector_region.shape, image.binning)]
        mosaiced_data = CCDData(data=np.zeros(binned_shape, dtype=image.data_type),
                                meta=image.primary_hdu.meta)
        mosaiced_data.binning = image.binning
        mosaiced_data.detector_section = mosaiced_detector_region
        for data in image.ccd_hdus:
            mosaiced_data.copy_in(data)
            image.remove(data)
        image.primary_hdu = mosaiced_data
        return image
