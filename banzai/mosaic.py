import logging
import numpy as np

from banzai.stages import Stage
from banzai.utils.image_utils import Section
from banzai.data import CCDData

logger = logging.getLogger('banzai')


class MosaicCreator(Stage):
    def __init__(self, runtime_context):
        super(MosaicCreator, self).__init__(runtime_context)

    def do_stage(self, image):
        logger.info('Mosaicing image', image=image)
        mosaiced_detector_region = self.get_mosaic_detector_region(image)
        binned_shape = [length // binning for length, binning in zip(mosaiced_detector_region.shape, image.binning)]
        mosaiced_data = CCDData(data=np.zeros(binned_shape, dtype=image.data_type),
                                meta=image.primary_hdu.meta)
        mosaiced_data.binning = image.binning
        mosaiced_data.detector_section = mosaiced_detector_region
        mosaiced_data.data_section = Section(x_start=1, y_start=1, x_stop=binned_shape[1], y_stop=binned_shape[0])
        mosaiced_data.extension_name = 'SCI'

        mosaiced_data.gain = np.mean([data.meta.get('GAIN') for data in image.ccd_hdus])
        mosaiced_data.saturate = np.min([data.saturate for data in image.ccd_hdus])
        mosaiced_data.max_linearity = np.min([data.max_linearity for data in image.ccd_hdus])

        for data in image.ccd_hdus:
            mosaiced_data.copy_in(data)
            image.remove(data)

        image.primary_hdu = mosaiced_data
        return image

    @staticmethod
    def get_mosaic_detector_region(image):
        x_detector_sections = []
        y_detector_sections = []
        for hdu in image.ccd_hdus:
            detector_section = Section.parse_region_keyword(hdu.meta.get('DETSEC', 'N/A'))
            x_detector_sections += [detector_section.x_start, detector_section.x_stop]
            y_detector_sections += [detector_section.y_start, detector_section.y_stop]
        return Section(min(x_detector_sections), max(x_detector_sections),
                       min(y_detector_sections), max(y_detector_sections))
