import numpy as np
from astropy.table import Table
from astropy.io import fits
import os

from banzai.utils import fits_utils
from banzai.tests.utils import FakeContext

fits_queue_message = {
    'path': '/archive/engineering/lsc/fa15/20200114/processed/lsc1m005-fa15-20200114-0129-d91.fits.fz'}

archived_fits_queue_message = {'SITEID': 'lsc',
                               'INSTRUME': 'fa15',
                               'DAY-OBS': '20200114',
                               'RLEVEL': 91,
                               'filename': 'lsc1m005-fa15-20200114-0129-d91.fits.fz'}

master_file_info = {'frameid': 1234,
                    'path': '/archive/engineering/lsc/fa15/20200114/processed/lsc1m005-fa15-20200114-skyflat-center-bin2x2-w.fits.fz'}


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




def test_get_filename_from_info_fits_queue():
    filename = fits_utils.get_filename_from_info(fits_queue_message)

    assert filename == 'lsc1m005-fa15-20200114-0129-d91.fits.fz'


def test_get_filename_from_info_archived_fits_queue():
    filename = fits_utils.get_filename_from_info(archived_fits_queue_message)

    assert filename == 'lsc1m005-fa15-20200114-0129-d91.fits.fz'


def test_get_local_path_from_info_fits_queue():
    context = FakeContext()

    filepath = fits_utils.get_local_path_from_info(fits_queue_message, context)
    base_filename, file_extension = os.path.splitext(os.path.basename(filepath))

    assert filepath == fits_queue_message['path']
    assert base_filename == 'lsc1m005-fa15-20200114-0129-d91.fits'
    assert file_extension == '.fz'


def test_get_local_path_from_info_archived_fits_queue():
    context = FakeContext()

    filepath = fits_utils.get_local_path_from_info(archived_fits_queue_message, context)
    base_filename, file_extension = os.path.splitext(os.path.basename(filepath))

    assert filepath == '/tmp/lsc/fa15/20200114/processed/lsc1m005-fa15-20200114-0129-d91.fits.fz'
    assert base_filename == 'lsc1m005-fa15-20200114-0129-d91.fits'
    assert file_extension == '.fz'


def test_get_local_path_from_master_cal():
    context = FakeContext()

    filepath = fits_utils.get_local_path_from_info(master_file_info, context)
    base_filename, file_extension = os.path.splitext(os.path.basename(filepath))

    assert filepath == '/archive/engineering/lsc/fa15/20200114/processed/lsc1m005-fa15-20200114-skyflat-center-bin2x2-w.fits.fz'
    assert base_filename == 'lsc1m005-fa15-20200114-skyflat-center-bin2x2-w.fits'
    assert file_extension == '.fz'


def test_is_s3_queue_message_archived_fits():
    assert fits_utils.is_s3_queue_message(archived_fits_queue_message)


def test_is_s3_queue_message_fits_queue():
    assert not fits_utils.is_s3_queue_message(fits_queue_message)


def test_get_basename_filename_fpacked():
    assert fits_utils.get_basename('foo.fits.fz') == 'foo'


def test_get_basename_fits():
    assert fits_utils.get_basename('foo.fits') == 'foo'


def test_get_basename_filepath():
    assert fits_utils.get_basename('/foo/bar/baz.fits.fz') == 'baz'
