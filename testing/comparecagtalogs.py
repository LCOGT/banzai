import glob
import os
import sys

from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.table import Table
from astropy.wcs import WCS
import logging
import numpy as np
from astropy import units as u
from py._error import Error

log = logging.getLogger(__name__)

logging.basicConfig(level=getattr(logging, 'DEBUG'),
                    format='%(asctime)s.%(msecs).03d %(levelname)7s: %(module)20s: %(message)s')


def get_source_catalog(imagename):
    e91image = fits.open(imagename)
    l1fwhm = e91image['SCI'].header['L1FWHM']
    exptime = e91image['SCI'].header['EXPTIME']
    defocus=  e91image['SCI'].header['FOCDMD']

    try:
        sourceCatalog = e91image['CAT'].data
        sourceCatalog = sourceCatalog[sourceCatalog['FLUX'] > 0]
        sourceCatalog['x'] = sourceCatalog['xwin']
        sourceCatalog['y'] = sourceCatalog['ywin']
        sourceCatalog['FLUX'] = -2.5 * np.log10(sourceCatalog['FLUX'] / exptime)


    except Error as e:
        log.warning("%s - No extension \'CAT\' available, skipping. %s" % (e91image, e))
        e91image.close()
        return (None, None, None)

    # instantiate the initial guess WCS from the image header
    image_wcs = WCS(e91image['SCI'].header)
    e91image.close()
    return (sourceCatalog, image_wcs, l1fwhm, defocus)


def comparebanzaicatalogs(image1, image2):
    if not os.path.isfile(image1) or not os.path.isfile(image2):
        return
    cat1, wcs1, fwhm1, defocus1 = get_source_catalog(image1)
    cat2, wcs2, fwhm2, defocus2 = get_source_catalog(image2)

    try:
        Coords1 = SkyCoord(ra=cat1['RA'] * u.degree, dec=cat1['Dec'] * u.degree)
        Coords2 = SkyCoord(ra=cat2['RA'] * u.degree, dec=cat2['Dec'] * u.degree)
        idx, d2d, d3d = Coords1.match_to_catalog_sky(Coords2)
        distance = Coords1.separation(Coords2[idx]).arcsecond
        matchcondition = (distance < 2)
        matchedCatalog = Table([cat1['FLUX'][matchcondition],
                            cat2['FLUX'][idx][matchcondition],

                            ],
                           names=['mag1', 'mag2']
                           )

        deltamag = np.median(matchedCatalog['mag1'] - matchedCatalog['mag2'])
    except:
        deltamag = np.NAN
        #print ("{} --- Skipped".format(os.path.basename (image2)) )
        #return


    print("{} {: 5.2f} {: 5.2f} {: 5.2f} | {: 6.4f} {: 6.4f} | {: 7d}  {: 7d} |"
          " {: 6.4f} {: 5.2f} {: 5.2f} ".format(os.path.basename (image2), defocus1, fwhm1, fwhm2,
                                              (wcs1.wcs.crval[
                                                   0] -
                                               wcs2.wcs.crval[
                                                   0]) * 3600,
                                              (wcs1.wcs.crval[
                                                   1] -
                                               wcs2.wcs.crval[
                                                   1]) * 3600,
                                              len(cat1),
                                              len(cat2),
                                              deltamag,
                                              np.max(cat1[
                                                         'FLUX']),
                                              np.max(cat2[
                                                         'FLUX']))
          )





path1 = sys.argv[1]
path2 = sys.argv[2]

inputfiles = sorted(glob.glob ("{}/{}".format (path2, "*e91*")))

for image2 in inputfiles:
    image1=image2.replace (path2,path1)

    comparebanzaicatalogs(image1, image2)
