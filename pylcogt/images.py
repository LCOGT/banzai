from astropy.io import fits
from astropy.coordinates import SkyCoord
from astropy import units
from pylcogt.utils import date_utils
from pylcogt.utils.fits_utils import table_to_fits
from pylcogt import dbs
import numpy as np


class Image(object):
    nx = 0
    ny = 0
    site = ''
    instrument = ''
    epoch = ''
    filename = ''
    ccdsum = ''
    filter = ''
    telescope_id = ''
    obstype = ''
    nx = 0
    ny = 0
    exptime = 0
    dateobs = None
    readnoise = 0.0
    catalog = None
    ra = 0.0
    dec = 0.0
    pixel_scale = 0.0

    def __init__(self, data=None, header=None, filename=None):

        if filename is not None:
            hdu = fits.open(filename, 'readonly')
            data = hdu[0].data.astype(np.float)
            header = hdu[0].header
            hdu.close()
            self.filename = filename

        self.data = data
        self.header = header

        if header is not None:
            self.parse_header(self.header)

    def parse_header(self, header):
        self.site = header['SITEID']
        self.instrument = header['INSTRUME']
        self.epoch = header['DAY-OBS']
        self.nx = header['NAXIS1']
        self.ny = header['NAXIS2']

        self.ccdsum = header['CCDSUM']
        self.filter = header['FILTER']
        self.telescope_id = dbs.get_telescope_id(self.site, self.instrument)
        self.obstype = header['OBSTYPE']
        self.exptime = float(header['EXPTIME'])
        self.dateobs = date_utils.parse_date_obs(header['DATE-OBS'])
        self.readnoise = float(header['RDNOISE'])
        coord = SkyCoord(header['RA'], header['DEC'], unit=(units.hourangle, units.degree))
        self.ra = coord.ra.deg
        self.dec = coord.dec.deg
        self.pixel_scale = float(header['PIXSCALE'])

    def subtract(self, value):
        self.data -= value

    def writeto(self, filename):
        table_hdu = table_to_fits(self.catalog)
        image_hdu = fits.PrimaryHDU(self.data, header=self.header)
        image_hdu.header['EXTEND'] = True
        hdu_list = fits.HDUList([image_hdu, table_hdu])
        hdu_list.writeto(filename, clobber=True)

    def update_shape(self, nx, ny):
        self.nx = nx
        self.ny = ny

    def write_catalog(self, filename, nsources=None):
        self.catalog[:nsources].write(filename, format='fits', overwrite=True)

    def add_history(self, msg):
        self.header.add_history(msg)


class InhomogeneousSetException(Exception):
    pass


def check_image_homogeneity(images):
    for attribute in ('nx', 'ny', 'ccdsum', 'epoch', 'site', 'instrument'):
        if len({getattr(image, attribute) for image in images}) > 1:
            raise InhomogeneousSetException('Images have different {}s'.format(attribute))
    return images[0]
