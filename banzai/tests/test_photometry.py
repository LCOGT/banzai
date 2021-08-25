from banzai.utils import photometry_utils
import numpy as np
from astropy.coordinates import SkyCoord
from astropy import units
from astropy.table import Table
from astropy.utils.data import get_pkg_data_filename
from astropy.io import fits


def test_match_catalog():
    np.random.seed(10142134)
    # make a few hundred coordinates
    reference_coordinates = SkyCoord(ra=np.random.uniform(30, 31, size=200), dec=np.random.uniform(11, 12, size=200),
                                     unit=(units.deg, units.deg))
    # choose about a hundred as "overlapping sources"
    reference_catalog = Table({'ra': [row.ra.value for row in reference_coordinates],
                               'dec': [row.dec.value for row in reference_coordinates],
                               'source_id': [i for i, _ in enumerate(reference_coordinates)]})

    input_catalog = {'ra': [row['ra'] for row in reference_catalog[:100]],
                     'dec': [row['dec'] for row in reference_catalog[:100]]}

    # Add ~10 spurious sources in the input catalog
    for ra, dec in zip(np.random.uniform(30, 31, size=10), np.random.uniform(11, 12, size=10)):
        input_catalog['ra'].append(ra)
        input_catalog['dec'].append(dec)

    # shuffle the input catalog, keeping track of the input id
    input_catalog = Table(input_catalog)
    # apparently shuffle does weird things to astropy tables so I just used the indexes here.
    indexes = np.arange(110)
    np.random.shuffle(indexes)
    input_catalog = input_catalog[indexes]

    # randomly offset the overlapping source positions in the input catalog by ~0.1 arcseconds
    for row in input_catalog:
        coordinate = SkyCoord(row['ra'], row['dec'], unit=(units.deg, units.deg))
        position_angle = np.random.uniform(0, 360) * units.deg
        separation = np.random.uniform(0.0, 0.3) * units.arcsec
        shifted_coordinate = coordinate.directional_offset_by(position_angle, separation)
        row['ra'], row['dec'] = shifted_coordinate.ra.value, shifted_coordinate.dec.value

    # make sure the overlapping sources are the same as we would expect
    matched_catalog = photometry_utils.match_catalogs(input_catalog, reference_catalog)
    for i in range(100):
        assert i in matched_catalog['source_id']
    assert len(matched_catalog) == 100


def test_photometric_calibration():
    # Note these values were measured by Daniel Harbeck using the photzp code
    cpt_catalog_filename = get_pkg_data_filename('data/cpt1m012-fa06-20200113-0102-matched-cat.fits', 'banzai.tests')
    matched_catalog = Table(fits.open(cpt_catalog_filename)[1].data)
    zeropoint, _, _, _ = photometry_utils.fit_photometry(matched_catalog, 'i', 'r-i', 250.032)
    assert np.abs(zeropoint - 23.1) < 0.1

    ogg_catalog_filename = get_pkg_data_filename('data/ogg2m001-ep04-20201006-0097-matched-cat.fits', 'banzai.tests')
    matched_catalog = Table(fits.open(ogg_catalog_filename)[1].data)
    zeropoint, _, _, _ = photometry_utils.fit_photometry(matched_catalog, 'i', 'r-i', 250.032)
    assert np.abs(zeropoint - 25.15) < 0.1
