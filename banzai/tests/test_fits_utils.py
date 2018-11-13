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


def test_get_sci_extensions():
    hdulist = [fits.PrimaryHDU(header=fits.Header({'test': 'test'}))]

    input_data = []
    for i in range(4):
        data = np.random.uniform(0, 1, size=(101, 101)).astype(dtype=np.float32)
        input_data.append(data)
        # Build the fits header manually because of a bug in the latest stable version of astropy
        # This should be
        # header = fits.Header({'EXTNAME': 'SCI', 'EXTVER': i + 1})
        header = fits.Header()
        header['EXTNAME'] = 'SCI'
        header['EXTVER'] = i + 1
        hdulist.append(fits.ImageHDU(data=data, header=header))

    bpm_header = fits.Header()
    bpm_header['EXTNAME'] = 'BPM'
    bpm_hdu = fits.ImageHDU(data=np.zeros((101, 101), dtype=np.uint8), header=bpm_header)
    hdulist.append(bpm_hdu)

    hdulist = fits.HDUList(hdulist)

    sci_extensions = fits_utils.get_extensions_by_name(hdulist, 'SCI')

    assert len(sci_extensions) == 4
    for i in range(4):
        np.testing.assert_allclose(sci_extensions[i].data, input_data[i], atol=1e-5)


def test_open_image():
    for fpacked in [True, False]:
        # Read an image with only a single extension
        # Read an images with a single extension and a BPM extension
        # Read an image with a single extension and a datacube
        # Read an image with multiple sci extensions
        pass
