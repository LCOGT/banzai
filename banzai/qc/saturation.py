import os

from banzai.stages import Stage
from banzai import logs


class SaturationTest(Stage):
    """
    Reject any images that have 5% or more of their pixels saturated.

    Notes
    =====
    Typically this means that something went wrong and can lead to bad master flat fields, etc.
    """
    # Empirically we have decided to use a 5% threshold to reject the image
    SATURATION_THRESHOLD = 0.05

    def __init__(self, pipeline_context):
        super(SaturationTest, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        images_to_remove = []
        for image in images:
            saturation_level = float(image.header['SATURATE'])
            saturated_pixels = image.data >= saturation_level
            total_pixels = image.data.size
            saturation_fraction = float(saturated_pixels.sum()) / total_pixels

            logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            logs.add_tag(logging_tags, 'SATFRAC', saturation_fraction)
            logs.add_tag(logging_tags, 'threshold', self.SATURATION_THRESHOLD)
            self.logger.info('Measured saturation fraction.', extra=logging_tags)
            saturated = saturation_fraction >= self.SATURATION_THRESHOLD
            if saturated:
                self.logger.error('SATFRAC exceeds threshold.', extra=logging_tags)
                images_to_remove.append(image)
            else:
                image.header['SATFRAC'] = (saturation_fraction,
                                           "Fraction of Pixels that are Saturated")

            self.save_qc_results({'Saturated': saturated,
                                 'saturation_fraction': saturation_fraction}, image)
        for image in images_to_remove:
            images.remove(image)

        return images
