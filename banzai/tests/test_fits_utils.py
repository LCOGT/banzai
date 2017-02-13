from __future__ import absolute_import, division, print_function, unicode_literals
import numpy as np
from astropy.table import Table
from astropy.io import fits
from banzai.utils import fits_utils


def test_table_to_fits():
    a = np.random.normal(size=100)
    b = np.random.normal(size=100)
    c = np.random.normal(size=100)

    t = Table([a,b,c], names=('a', 'b', 'c'))

    t['a'].description = 'Column a'
    t['b'].description = 'Column b'
    t['c'].description = 'Column c'
    t['a'].unit = 'pix'
    t['b'].unit = 'ct'
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
    assert hdu.header['TUNIT2'] == 'ct'
    assert hdu.header['TUNIT3'] == 's'
    assert hdu.header.cards['TTYPE1'].comment == 'Column a'
    assert hdu.header.cards['TTYPE2'].comment == 'Column b'
    assert hdu.header.cards['TTYPE3'].comment == 'Column c'


def test_get_sci_extensions():
    hdu0 = fits.PrimaryHDU(header=fits.Header({'test': 'test'}))
    data1 = np.random.uniform(0, 1, size=(101, 101)).astype(dtype=np.float32)

    hdu1 = fits.ImageHDU(data=data1, header=fits.Header({'EXTNAME': 'SCI', 'EXTVER': 1}))
    data2 = np.random.uniform(0, 1, size=(101, 101)).astype(dtype=np.float32)
    hdu2 = fits.ImageHDU(data=data2, header=fits.Header({'EXTNAME': 'SCI', 'EXTVER': 2}))
    data3 = np.random.uniform(0, 1, size=(101, 101)).astype(dtype=np.float32)
    hdu3 = fits.ImageHDU(data=data3, header=fits.Header({'EXTNAME': 'SCI', 'EXTVER': 3}))
    data4 = np.random.uniform(0, 1, size=(101, 101)).astype(dtype=np.float32)
    hdu4 = fits.ImageHDU(data=data4, header=fits.Header({'EXTNAME': 'SCI', 'EXTVER': 4}))

    bpm_hdu = fits.ImageHDU(data=np.zeros((101, 101), dtype=np.uint8),
                            header=fits.Header({'EXTNAME': 'BPM', 'EXTVER': 1}))

    hdulist = fits.HDUList([hdu0, hdu1, hdu2, hdu3, hdu4, bpm_hdu])
    sci_extensions = fits_utils.get_sci_extensions(hdulist)
    assert len(sci_extensions) == 4
    np.testing.assert_allclose(sci_extensions[0].data, data1, atol=1e-5)
    np.testing.assert_allclose(sci_extensions[1].data, data2, atol=1e-5)
    np.testing.assert_allclose(sci_extensions[2].data, data3, atol=1e-5)
    np.testing.assert_allclose(sci_extensions[3].data, data4, atol=1e-5)

def test_open_image():
    for fpacked in [True, False]:
        # Read an image with only a single extension
        # Read an images with a single extension and a BPM extension
        # Read an image with a single extension and a datacube
        # Read an image with multiple sci extensions
        pass