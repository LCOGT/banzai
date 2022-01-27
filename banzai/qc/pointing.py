import logging

import astropy.units as u
from astropy.coordinates import SkyCoord

from banzai.stages import Stage
from banzai.utils import qc

logger = logging.getLogger('banzai')


class PointingTest(Stage):
    """
    A test to determine  whether or not the poiting error on the frame
    (as determined by a WCS solve) is within tolerance.
    """

    # Typical pointing is within 5" of requested pointing (decimal degrees).
    WARNING_THRESHOLD = 30.0
    SEVERE_THRESHOLD = 300.0

    def __init__(self, runtime_context):
        super(PointingTest, self).__init__(runtime_context)

    def do_stage(self, image):
        try:
            # OFST-RA/DEC is the same as CAT-RA/DEC but includes user requested offset
            requested_coords = SkyCoord(image.meta['OFST-RA'], image.meta['OFST-DEC'],
                                        unit=(u.hour, u.deg), frame='icrs')
        except ValueError as e:
            try:
                # Fallback to CAT-RA and CAT-DEC
                requested_coords = SkyCoord(image.meta['CAT-RA'], image.meta['CAT-DEC'],
                                            unit=(u.hour, u.deg), frame='icrs')
            except:
                logger.error(e, image=image)
                return image
        # This only works assuming CRPIX is at the center of the image
        solved_coords = SkyCoord(image.meta['CRVAL1'], image.meta['CRVAL2'],
                                 unit=(u.deg, u.deg), frame='icrs')

        angular_separation = solved_coords.separation(requested_coords).arcsec

        logging_tags = {'PNTOFST': angular_separation,
                        'OBJECT': image.meta.get('OBJECT', 'N/A')}

        pointing_severe = abs(angular_separation) > self.SEVERE_THRESHOLD
        pointing_warning = abs(angular_separation) > self.WARNING_THRESHOLD
        if pointing_severe:
            logger.error('Pointing offset exceeds threshold', image=image, extra_tags=logging_tags)
        elif pointing_warning:
            logger.warning('Pointing offset exceeds threshhold', image=image, extra_tags=logging_tags)
        qc_results = {'pointing.failed': pointing_severe,
                      'pointing.failed_threshold': self.SEVERE_THRESHOLD,
                      'pointing.warning': pointing_warning,
                      'pointing.warning_threshold': self.WARNING_THRESHOLD,
                      'pointing.offset': angular_separation}
        qc.save_qc_results(self.runtime_context, qc_results, image)

        image.meta['PNTOFST'] = (angular_separation, '[arcsec] offset of requested and solved center')

        return image
