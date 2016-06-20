from __future__ import absolute_import, division, print_function, unicode_literals

from banzai import astrometry
from astropy import units
from astropy.coordinates import SkyCoord


def test_ra_dec_string_conversion():
    ra = '20:14:06.0234'
    dec = '+10:11:31.213'
    coord = SkyCoord(ra + dec, unit=(units.hourangle, units.deg))
    converted_ra, converted_dec = astrometry.get_ra_dec_in_sexagesimal(coord.ra.deg, coord.dec.deg)
    assert converted_ra == ra
    assert converted_dec == dec
