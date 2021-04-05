import numpy as np
from astropy.io.fits import Header
from astropy.table import Table
from astropy.io import fits
import pytest

from banzai.utils import fits_utils
from banzai.tests.utils import FakeContext

pytestmark = pytest.mark.fits_utils


def test_table_to_fits():
    a = np.random.normal(size=100)
    b = np.random.normal(size=100)
    c = np.random.normal(size=100)

    t = Table([a,b,c], names=('a', 'b', 'c'))

    t['a'].description = 'Column a'
    t['b'].description = 'Column b'
    t['c'].description = 'Column c'
    t['a'].unit = 'pix'
    t['b'].unit = 'count'
    t['c'].unit = 's'

    hdu = fits_utils.table_to_fits(t)

    assert hdu.__class__ == fits.BinTableHDU
    np.testing.assert_array_equal(hdu.data['A'], a)
    np.testing.assert_array_equal(hdu.data['B'], b)
    np.testing.assert_array_equal(hdu.data['C'], c)
    assert hdu.header['TCOMM1'] == 'Column a'
    assert hdu.header['TCOMM2'] == 'Column b'
    assert hdu.header['TCOMM3'] == 'Column c'
    assert hdu.header['TUNIT1'] == 'pix'
    assert hdu.header['TUNIT2'] == 'count'
    assert hdu.header['TUNIT3'] == 's'
    assert hdu.header.cards['TTYPE1'].comment == 'Column a'
    assert hdu.header.cards['TTYPE2'].comment == 'Column b'
    assert hdu.header.cards['TTYPE3'].comment == 'Column c'


def test_sanitize_header():
    header = Header({'CCDSUM': '1 1',
                     'SIMPLE': 'foo',
                     'BITPIX': 8,
                     'NAXIS': 2,
                     'CHECKSUM': 'asdf'})

    sanitized_header = fits_utils.sanitize_header(header)

    assert list(sanitized_header.keys()) == ['CCDSUM']


def test_get_configuration_mode_na():
    header = Header({'CONFMODE': 'N/A'})

    configuration_mode = fits_utils.get_configuration_mode(header)

    assert configuration_mode == 'default'


def test_get_configuration_mode_none():
    header = Header({})

    configuration_mode = fits_utils.get_configuration_mode(header)

    assert configuration_mode == 'default'


def test_get_configuration_mode_central_2k():
    header = Header({'CONFMODE': 'central_2k_2x2'})

    configuration_mode = fits_utils.get_configuration_mode(header)

    assert configuration_mode == 'central_2k_2x2'


def test_pack_all_extensions_compressed():
    expected_fpacked_extensions = ['SCI', 'BPM', 'ERR']
    hdu_list = fits.HDUList([fits.ImageHDU(data=np.zeros(100), header=Header({'EXTNAME': 'SCI'})),
                             fits.ImageHDU(data=np.zeros(100), header=Header({'EXTNAME': 'BPM'})),
                             fits.ImageHDU(data=np.zeros(100), header=Header({'EXTNAME': 'ERR'}))])

    fpacked_hdulist = fits_utils.pack(hdu_list, FakeContext())
    for extname in expected_fpacked_extensions:
        assert isinstance(fpacked_hdulist[extname], fits.CompImageHDU)

def test_pack_some_extensions_compressed():
    hdu_list = fits.HDUList([fits.ImageHDU(data=np.zeros(100), header=Header({'EXTNAME': 'SCI'})),
                             fits.ImageHDU(data=np.zeros(100), header=Header({'EXTNAME': 'BPM'})),
                             fits.ImageHDU(data=np.zeros(100), header=Header({'EXTNAME': 'TEST'}))])

    fpacked_hdulist = fits_utils.pack(hdu_list, FakeContext())
    assert isinstance(fpacked_hdulist['TEST'], fits.ImageHDU)


def test_open_image():
    for fpacked in [True, False]:
        # Read an image with only a single extension
        # Read an images with a single extension and a BPM extension
        # Read an image with a single extension and a datacube
        # Read an image with multiple sci extensions
        pass
