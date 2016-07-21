from banzai.stages import Stage
from banzai import logs

import os

class SaturationTest(Stage):
    """

    """
    # Empirically we have decided to use a 5% threshold to reject the image
    threshold = 0.05

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
            self.logger.info('Measured saturation fraction.', extra=logging_tags)
            if saturation_fraction >= self.threshold:
                self.logger.error('SATFRAC exceeds threshold.', extra=logging_tags)
                images_to_remove.append(image)
            else:
                image.header['SATFRAC'] = (saturation_fraction, "Fraction of Pixels that are Saturated")

        for image in images_to_remove:
            images.remove(image)

        return images
