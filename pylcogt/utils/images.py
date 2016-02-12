from astropy.io import fits
from pylcogt.utils import date_utils
from pylcogt import dbs
import numpy as np

class Image(object):
    def __init__(self, filename):

        hdu = fits.open(filename, 'readonly')
        self.data = hdu[0].data.astype(np.float)
        self.header = hdu[0].header
        self.site = hdu[0].header['SITEID']
        self.instrument = hdu[0].header['INSTRUME']
        self.epoch = hdu[0].header['DAY-OBS']
        self.nx = hdu[0].header['NAXIS1']
        self.ny = hdu[0].header['NAXIS2']
        self.filename = filename
        self.ccdsum = hdu[0].header['CCDSUM']
        self.filter = hdu[0].header['FILTER']
        self.telescope_id = dbs.get_telescope_id(self.site, self.instrument)
        self.obstype = hdu[0].header['OBSTYPE']
        self.exptime = float(hdu[0].header['EXPTIME'])
        self.dateobs = date_utils.parse_date_obs(hdu[0].header['DATE-OBS'])
        self.readnoise = float(hdu[0].header['RDNOISE'])
        self.catalog = None

    def subtract(self, value):
        return self.data - value

    def writeto(self, filename):
        image_hdu =fits.ImageHDU(self.data, header=self.header)
        hdu_list = fits.HDUList([image_hdu, self.catalog])
        hdu_list.writeto(filename, clobber=True)

    def update_shape(self, nx, ny):
        self.nx = nx
        self.ny = ny

    def write_catalog(self, filename):
        self.catalog.write(filename, format='fits', overwrite=True)