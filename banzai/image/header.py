from banzai import logs
from banzai.utils import date_utils, fits_utils

logger = logs.get_logger(__name__)

class Header(object):
    def __init__(self, header: dict):
        self.request_number = header['REQNUM']
        self.epoch = str(header['DAY-OBS'])
        self.nx = header['NAXIS1']
        self.ny = header['NAXIS2']
        self.block_id = header['BLKUID']
        self.molecule_id = header['MOLUID']
        self.ccdsum = header['CCDSUM']
        self.filter = header['FILTER']
        self.obstype = header['OBSTYPE']
        self.gain = eval(str(header['GAIN']))
        self.site = header['SITEID']
        self.instrument = header['INSTRUME']

        self.exptime = float(header.get('EXPTIME', 0.0))
        self.dateobs = date_utils.parse_date_obs(header.get('DATE-OBS', '1900-01-01T00:00:00.00000'))
        self.readnoise = float(header.get('RDNOISE', 0.0))
        self.ra, self.dec = fits_utils.parse_ra_dec(header)
        self.pixel_scale = float(header.get('PIXSCALE', 0.0))
