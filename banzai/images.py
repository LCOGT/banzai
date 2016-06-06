from astropy.io import fits
from banzai.utils import date_utils
from banzai.utils import fits_utils
from banzai import dbs
import numpy as np
import os


class Image(object):

    def __init__(self, filename=None, data=None, header={}, bpm=None):

        if filename is not None:
            data, header, bpm = fits_utils.open_image(filename)
            if '.fz' == filename[-3:]:
                filename = filename[:-3]
            self.filename = os.path.basename(filename)

        self.data = data
        self.header = header
        self.bpm = bpm

        self.site = header.get('SITEID')
        self.instrument = header.get('INSTRUME')
        self.epoch = header.get('DAY-OBS')
        self.nx = header.get('NAXIS1')
        self.ny = header.get('NAXIS2')

        self.gain = header.get('GAIN')
        self.ccdsum = header.get('CCDSUM')
        self.filter = header.get('FILTER')
        self.telescope_id = dbs.get_telescope_id(self.site, self.instrument)

        self.obstype = header.get('OBSTYPE')
        self.exptime = float(header.get('EXPTIME'))
        self.dateobs = date_utils.parse_date_obs(header.get('DATE-OBS'))
        self.readnoise = float(header.get('RDNOISE'))
        self.ra, self.dec = fits_utils.parse_ra_dec(header)
        self.pixel_scale = float(header.get('PIXSCALE'))
        self.catalog = None

    def subtract(self, value):
        self.data -= value

    def writeto(self, filename, fpack=False):
        image_hdu = fits.PrimaryHDU(self.data.astype(np.float32), header=self.header)
        image_hdu.header['EXTEND'] = True
        image_hdu.update_ext_name('SCI')
        hdu_list = [image_hdu]
        if self.catalog is not None:
            table_hdu = fits_utils.table_to_fits(self.catalog)
            table_hdu.update_ext_name('CAT')
            hdu_list.append(table_hdu)
        if self.bpm is not None:
            bpm_hdu = fits.ImageHDU(self.bpm.astype(np.uint8))
            bpm_hdu.update_ext_name('BPM')
            hdu_list.append(bpm_hdu)

        hdu_list = fits.HDUList(hdu_list)
        hdu_list.writeto(filename, clobber=True)
        if fpack:
            if os.path.exists(filename + '.fz'):
                os.remove(filename + '.fz')
            os.system('fpack -q 64 {0}'.format(filename))
            os.remove(filename)
            self.filename += '.fz'

    def update_shape(self, nx, ny):
        self.nx = nx
        self.ny = ny

    def write_catalog(self, filename, nsources=None):
        if self.catalog is None:
            raise MissingCatalogException
        else:
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


class MissingCatalogException(Exception):
    pass
