import os
import astropy.units as u
from astropy.coordinates import SkyCoord

from banzai.stages import Stage
from banzai import logs


class PointingTest(Stage):
    """
    A test to determine  whether or not the poiting error on the frame
    (as determined by a WCS solve) is within tolerance.
    """

    # Typical poiting is within 30" of requested pointing (decimal degrees).
    # Error threshold is over 100".
    WARNING_THRESHOLD = 5.0
    SEVERE_THRESHOLD = 30.0

    def __init__(self, pipeline_context):
        super(PointingTest, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def setup_logging(self, image):
        self.logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
        logs.add_tag(self.logging_tags, 'filename', os.path.basename(image.filename))

    def do_stage(self, images):
        for image in images:
            self.setup_logging(image)

            # OFST-RA/DEC is the same as CAT-RA/DEC but includes user requested offset
            requested_coords = SkyCoord(
                image.header['OFST-RA'],
                image.header['OFST-DEC'],
                unit=(u.hour, u.deg),
                frame='icrs'
            )
            # This only works assuming CRPIX is at the center of the image
            solved_coords = SkyCoord(
                image.header['CRVAL1'],
                image.header['CRVAL2'],
                unit=(u.deg, u.deg),
                frame='icrs'
            )

            angular_separation = solved_coords.separation(requested_coords).arcsec

            logs.add_tag(self.logging_tags, 'PNTOFST', angular_separation)

            if abs(angular_separation) > self.SEVERE_THRESHOLD:
                self.logger.error('Pointing offset exceeds threshold', extra=self.logging_tags)
            elif abs(angular_separation) > self.WARNING_THRESHOLD:
                self.logger.warning('Pointing offset exceeds threshhold', extra=self.logging_tags)

            image.header['PNTOFST'] = (
                angular_separation, '[arcsec] offset of requested and solved center'
            )

        return images
