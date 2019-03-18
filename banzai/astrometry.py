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

from banzai.exceptions import MissingCatalogException
from banzai.stages import Stage

logger = logging.getLogger(__name__)

_ASTROMETRY_SERVICE_URL='http://astrometry.lco.gtn/catalog/'

class WCSSolver(Stage):

    def __init__(self, runtime_context):
        super(WCSSolver, self).__init__(runtime_context)

    def do_stage(self, image):

        # Skip the image if we don't have some kind of initial RA and Dec guess
        if np.isnan(image.ra) or np.isnan(image.dec):
            logger.error('Skipping WCS solution. No initial pointing guess from header.', image=image)
            image.header['WCSERR'] = (4, 'Error status of WCS fit. 0 for no error')
            return image

        with tempfile.TemporaryDirectory() as tmpdirname:
            filename = os.path.basename(image.filename)
            catalog_name = os.path.join(tmpdirname, filename.replace('.fits', '.cat.fits'))
            try:
                image.write_catalog(catalog_name, nsources=40)
            except MissingCatalogException:
                image.header['WCSERR'] = (4, 'Error status of WCS fit. 0 for no error')
                logger.error('No source catalog. Not attempting WCS solution', image=image)
                return image

            # Run astrometry.net
            wcs_name = os.path.join(tmpdirname, filename.replace('.fits', '.wcs.fits'))
            command = self.cmd.format(ra=image.ra, dec=image.dec, scale_low=0.9*image.pixel_scale,
                                      scale_high=1.1*image.pixel_scale, wcs_name=wcs_name,
                                      catalog_name=catalog_name, nx=image.nx, ny=image.ny)

            try:
                console_output = subprocess.check_output(shlex.split(command))
            except subprocess.CalledProcessError:
                logger.error('Astrometry.net threw an error.', image=image)

            logger.debug(console_output, image=image)

            if os.path.exists(wcs_name):
                # Copy the WCS keywords into original image
                new_header = fits.getheader(wcs_name)

                header_keywords_to_update = ['CTYPE1', 'CTYPE2', 'CRPIX1', 'CRPIX2', 'CRVAL1',
                                             'CRVAL2', 'CD1_1', 'CD1_2', 'CD2_1', 'CD2_2']
                for keyword in header_keywords_to_update:
                    image.header[keyword] = new_header[keyword]

                image.header['WCSERR'] = (0, 'Error status of WCS fit. 0 for no error')

                # Update the RA and Dec header keywords
                image.header['RA'], image.header['DEC'] = get_ra_dec_in_sexagesimal(image.header['CRVAL1'],
                                                                                    image.header['CRVAL2'])

                # Clean up wcs file
                os.remove(wcs_name)

                # Add the RA and Dec values to the catalog
                add_ra_dec_to_catalog(image)
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
