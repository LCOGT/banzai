import logging
import requests
from requests import ConnectionError, HTTPError

from astropy.wcs import WCS
from astropy.coordinates import SkyCoord
from astropy import units
import numpy as np

from banzai.stages import Stage
from banzai import settings

logger = logging.getLogger('banzai')

FAILED_WCS = (4, 'Error status of WCS fit. 0 for no error')
SUCCESSFUL_WCS = (0, 'Error status of WCS fit. 0 for no error')


class WCSSolver(Stage):

    def __init__(self, runtime_context):
        super(WCSSolver, self).__init__(runtime_context)

    def do_stage(self, image):

        # Skip the image if we don't have some kind of initial RA and Dec guess
        if np.isnan(image.ra) or np.isnan(image.dec):
            logger.error('Skipping WCS solution. No initial pointing guess from header.', image=image)
            image.meta['WCSERR'] = FAILED_WCS
            return image

        if image['CAT'] is None:
            logger.warning('Not attempting WCS solve because no catalog exists', image=image)
            image.meta['WCSERR'] = FAILED_WCS
            return image

        # Get the image catalog and sort in order of flux
        # with the brightest objects first
        image_catalog = image['CAT'].data
        image_catalog.sort('flux')
        image_catalog.reverse()

        catalog_payload = {'X': list(image_catalog['x'])[:settings.WCS_SOURCE_LIMIT],
                           'Y': list(image_catalog['y'])[:settings.WCS_SOURCE_LIMIT],
                           'FLUX': list(image_catalog['flux'])[:settings.WCS_SOURCE_LIMIT],
                           'radius': settings.WCS_RADIUS,
                           'tolerance': settings.WCS_TOLERANCE,
                           'pixel_scale': image.pixel_scale,
                           'naxis': 2,
                           'naxis1': image.shape[1],
                           'naxis2': image.shape[0],
                           'ra': image.ra,
                           'dec': image.dec,
                           'statistics': False,
                           'filename': image.filename}
        try:
            astrometry_response = requests.post(self.runtime_context.ASTROMETRY_SERVICE_URL, json=catalog_payload)
            astrometry_response.raise_for_status()
        except ConnectionError:
            logger.error('Astrometry service unreachable.', image=image)
            image.meta['WCSERR'] = FAILED_WCS
            return image
        except HTTPError:
            if astrometry_response.status_code == 400:
                logger.error('Astrometry service query malformed: {msg}'.format(msg=astrometry_response.json()),
                             image=image)
            else:
                try:
                    logger.error('Astrometry service encountered an error.', image=image,
                                 extra_tags={'astrometry_message': astrometry_response.json().get('message', '')})
                except:
                    logger.error('Astrometry service encountered an error.', image=image)

            image.meta['WCSERR'] = FAILED_WCS
            return image

        if not astrometry_response.json()['solved']:
            logger.warning('WCS solution failed.', image=image)
            image.meta['WCSERR'] = FAILED_WCS
            return image

        header_keywords_to_update = ['CTYPE1', 'CTYPE2', 'CRPIX1', 'CRPIX2', 'CRVAL1',
                                     'CRVAL2', 'CD1_1', 'CD1_2', 'CD2_1', 'CD2_2']

        for keyword in header_keywords_to_update:
            image.meta[keyword] = astrometry_response.json()[keyword]

        image.meta['RA'], image.meta['DEC'] = get_ra_dec_in_sexagesimal(image.meta['CRVAL1'],
                                                                        image.meta['CRVAL2'])

        add_ra_dec_to_catalog(image)

        image.meta['WCSERR'] = SUCCESSFUL_WCS

        logger.info('Attempted WCS Solve', image=image, extra_tags={'WCSERR': image.meta['WCSERR']})
        return image


def add_ra_dec_to_catalog(image):
    image_wcs = WCS(image.meta)
    image_catalog = image['CAT'].data
    ras, decs = image_wcs.all_pix2world(image_catalog['x'], image_catalog['y'], 1)

    image_catalog['ra'] = ras
    image_catalog['dec'] = decs
    image_catalog['ra'].unit = 'degree'
    image_catalog['dec'].unit = 'degree'
    image_catalog['ra'].description = 'Right Ascension'
    image_catalog['dec'].description = 'Declination'


def get_ra_dec_in_sexagesimal(ra, dec):
    """
    Convert a decimal RA and Dec to sexagesimal

    Parameters
    ----------
    ra : float
         Right Ascension in decimal form
    dec : float
         Declination in decimal form

    Returns
    -------
    tuple of str : RA, Dec converted to a string

    """
    coord = SkyCoord(ra, dec, unit=(units.deg, units.deg))
    coord_str = coord.to_string('hmsdms', precision=4, pad=True)
    ra_str, dec_str = coord_str.split()
    ra_str = ra_str.replace('h', ':').replace('m', ':').replace('s', '')
    dec_str = dec_str.replace('d', ':').replace('m', ':').replace('s', '')
    # Return one less digit of precision for the dec
    dec_str = dec_str[:-1]
    return ra_str, dec_str
