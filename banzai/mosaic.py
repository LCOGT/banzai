import numpy as np

from banzai.stages import Stage
from banzai.utils.image_utils import Section
from banzai.data import CCDData
from banzai.logs import get_logger

logger = get_logger()


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
        mosaiced_data.name = 'SCI'

        mosaiced_data.gain = np.mean([data.meta.get('GAIN') for data in image.ccd_hdus])
        mosaiced_data.saturate = np.min([data.saturate for data in image.ccd_hdus])
        mosaiced_data.max_linearity = np.min([data.max_linearity for data in image.ccd_hdus])

        # Store Overscan to header for each amplifier
        mosaiced_data.meta['L1STATOV'] = '1' if any([data.meta.get('L1STATOV', '0') == '1' for data in image.ccd_hdus]) \
                                         else '0', 'Status flag for overscan correction'

        for i, data in enumerate(image.ccd_hdus):
            mosaiced_data.copy_in(data)
            mosaiced_data.meta[f'OVERSCN{i + 1}'] = '{:0.2f}'.format(data.meta.get('OVERSCAN', 0.0)), \
                                                    'Overscan value that was subtracted'
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
