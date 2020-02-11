import logging

from banzai.stages import Stage
from banzai.utils import qc

logger = logging.getLogger('banzai')


class SaturationTest(Stage):
    """
    Reject any images that have 5% or more of their pixels saturated.

    Notes
    =====
    Typically this means that something went wrong and can lead to bad master flat fields, etc.
    """
    # Empirically we have decided to use a 5% threshold to reject the image
    SATURATION_THRESHOLD = 0.05

    def __init__(self, runtime_context):
        super(SaturationTest, self).__init__(runtime_context)

    def do_stage(self, image):
        saturation_level = float(image.meta['SATURATE'])
        saturated_pixels = image.data >= saturation_level
        total_pixels = image.data.size
        saturation_fraction = float(saturated_pixels.sum()) / total_pixels

        logging_tags = {'SATFRAC': saturation_fraction,
                        'threshold': self.SATURATION_THRESHOLD}
        logger.info('Measured saturation fraction.', image=image, extra_tags=logging_tags)
        is_saturated = saturation_fraction >= self.SATURATION_THRESHOLD
        qc_results = {'saturated.failed': is_saturated,
                      'saturated.fraction': saturation_fraction,
                      'saturated.threshold': self.SATURATION_THRESHOLD}
        if is_saturated:
            logger.error('SATFRAC exceeds threshold.', image=image, extra_tags=logging_tags)
            qc_results['rejected'] = True
        else:
            image.meta['SATFRAC'] = (saturation_fraction,
                                       "Fraction of Pixels that are Saturated")

        qc.save_qc_results(self.runtime_context, qc_results, image)
        return None if is_saturated else image
