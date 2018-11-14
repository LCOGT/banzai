import logging

import astropy.units as u
from astropy.coordinates import SkyCoord

from banzai.stages import Stage

logger = logging.getLogger(__name__)


class PointingTest(Stage):
    """
    A test to determine  whether or not the poiting error on the frame
    (as determined by a WCS solve) is within tolerance.
    """

    # Typical pointing is within 5" of requested pointing (decimal degrees).
    WARNING_THRESHOLD = 30.0
    SEVERE_THRESHOLD = 300.0

    def __init__(self, pipeline_context):
        super(PointingTest, self).__init__(pipeline_context)

    def do_stage(self, images):
        for image in images:
            try:
                # OFST-RA/DEC is the same as CAT-RA/DEC but includes user requested offset
                requested_coords = SkyCoord(image.header['OFST-RA'], image.header['OFST-DEC'],
                                            unit=(u.hour, u.deg), frame='icrs')
            except ValueError as e:
                try:
                    # Fallback to CAT-RA and CAT-DEC
                    requested_coords = SkyCoord(image.header['CAT-RA'], image.header['CAT-DEC'],
                                                unit=(u.hour, u.deg), frame='icrs')
                except:
                    logger.error(e, image=image)
                    continue

            # This only works assuming CRPIX is at the center of the image
            solved_coords = SkyCoord(image.header['CRVAL1'], image.header['CRVAL2'],
                                     unit=(u.deg, u.deg), frame='icrs')

            angular_separation = solved_coords.separation(requested_coords).arcsec

            logging_tags = {'PNTOFST': angular_separation}

            pointing_severe = abs(angular_separation) > self.SEVERE_THRESHOLD
            pointing_warning = abs(angular_separation) > self.WARNING_THRESHOLD
            if pointing_severe:
                logger.error('Pointing offset exceeds threshold', image=image, extra_tags=logging_tags)
            elif pointing_warning:
                logger.warning('Pointing offset exceeds threshhold', image=image, extra_tags=logging_tags)
            self.save_qc_results({'pointing.failed': pointing_severe,
                                  'pointing.failed_threshold': self.SEVERE_THRESHOLD,
                                  'pointing.warning': pointing_warning,
                                  'pointing.warning_threshold': self.WARNING_THRESHOLD,
                                  'pointing.offset': angular_separation},
                                 image)

            image.header['PNTOFST'] = (
                angular_separation, '[arcsec] offset of requested and solved center'
            )

        return images
