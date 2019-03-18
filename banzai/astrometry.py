import os
import subprocess
import shlex
import tempfile
import logging
import requests
from requests import ConnectionError, HTTPError

from astropy.io import fits
from astropy.wcs import WCS
from astropy.coordinates import SkyCoord
from astropy import units
import numpy as np

from banzai.stages import Stage
from banzai.utils import image_utils

logger = logging.getLogger(__name__)

_ASTROMETRY_SERVICE_URL='http://astrometry.lco.gtn/catalog/'

class WCSSolver(Stage):

    def __init__(self, pipeline_context):
        super(WCSSolver, self).__init__(pipeline_context)

    def do_stage(self, image):

        # Skip the image if we don't have some kind of initial RA and Dec guess
        if np.isnan(image.ra) or np.isnan(image.dec):
            logger.error('Skipping WCS solution. No initial pointing guess from header.', image=image)
            image.header['WCSERR'] = (4, 'Error status of WCS fit. 0 for no error')
            return image

        image_catalog = image.data_tables.get('catalog')

        catalog_payload = {'X': list(image_catalog['x']),
                           'Y': list(image_catalog['y']),
                           'FLUX': list(image_catalog['flux']),
                           'pixel_scale': image.pixel_scale,
                           'naxis': 2,
                           'naxis1': image.nx,
                           'naxis2': image.ny,
                           'ra': image.ra,
                           'dec': image.dec,
                           'statistics': False}
        try:
            astrometry_response = requests.post(_ASTROMETRY_SERVICE_URL, json=catalog_payload)
            astrometry_response.raise_for_status()
        except ConnectionError:
            logger.error('Astrometry service unreachable.', image=image)
            image.header['WCSERR'] = (4, 'Error status of WCS fit. 0 for no error')
            return image
        except HTTPError:
            if astrometry_response.status_code == 400:
                logger.error('Astrometry service query malformed', image=image)
            else:
                logger.error('Astrometry service encountered an error.', image=image)

            image.header['WCSERR'] = (4, 'Error status of WCS fit. 0 for no error')
            return image

        if astrometry_response.json()['solved'] == False:
            logger.warning('WCS solution failed.', image=image)
            image.header['WCSERR'] = (4, 'Error status of WCS fit. 0 for no error')
            return image

        header_keywords_to_update = ['CTYPE1', 'CTYPE2', 'CRPIX1', 'CRPIX2', 'CRVAL1',
                                     'CRVAL2', 'CD1_1', 'CD1_2', 'CD2_1', 'CD2_2']

        for keyword in header_keywords_to_update:
            image.header[keyword] = astrometry_response.json()[keyword]

        image.header['RA'], image.header['DEC'] = get_ra_dec_in_sexagesimal(image.header['CRVAL1'],
                                                                            image.header['CRVAL2'])

        add_ra_dec_to_catalog(image)

        image.header['WCSERR'] = (0, 'Error status of WCS fit. 0 for no error')

        logger.info('Attempted WCS Solve', image=image, extra_tags={'WCSERR': image.header['WCSERR']})
        return image


def add_ra_dec_to_catalog(image):
    image_wcs = WCS(image.header)
    ras, decs = image_wcs.all_pix2world(image.data_tables['catalog']['x'], image.data_tables['catalog']['y'], 1)
    image.data_tables['catalog']['ra'] = ras
    image.data_tables['catalog']['dec'] = decs
    image.data_tables['catalog']['ra'].unit = 'degree'
    image.data_tables['catalog']['dec'].unit = 'degree'
    image.data_tables['catalog']['ra'].description = 'Right Ascension'
    image.data_tables['catalog']['dec'].description = 'Declination'


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
