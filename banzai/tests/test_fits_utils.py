import numpy as np
from astropy.io.fits import Header
from astropy.table import Table
from astropy.io import fits
import pytest
import io
from unittest.mock import patch, MagicMock

from banzai.utils import fits_utils
from banzai.exceptions import FrameNotAvailableError

pytestmark = pytest.mark.fits_utils


class FakeContext:
    ARCHIVE_FRAME_URL = 'https://archive.example.com/frames'
    ARCHIVE_AUTH_HEADER = {'Authorization': 'Token test'}
    RAW_DATA_FRAME_URL = 'https://raw.example.com/frames'
    RAW_DATA_AUTH_HEADER = {'Authorization': 'Token test'}


def make_fits_bytes():
    buf = io.BytesIO()
    fits.HDUList([fits.PrimaryHDU()]).writeto(buf)
    return buf.getvalue()


@patch('banzai.utils.fits_utils.requests.get')
def test_download_from_s3_retries_on_400(mock_get):
    bad_response = MagicMock()
    bad_response.status_code = 400
    bad_response.raise_for_status.side_effect = Exception('400 Bad Request')

    good_response = MagicMock()
    good_response.status_code = 200
    good_response.raise_for_status.return_value = None
    good_response.json.return_value = {'url': 'https://s3.example.com/file.fits'}

    s3_response = MagicMock()
    s3_response.status_code = 200
    s3_response.raise_for_status.return_value = None
    s3_response.content = make_fits_bytes()

    mock_get.side_effect = [bad_response, good_response, s3_response]

    file_info = {'frameid': 42, 'filename': 'test.fits'}
    result = fits_utils.download_from_s3(file_info, FakeContext())

    assert mock_get.call_count == 3
    assert result.read(4) == b'SIMP'


@patch('banzai.utils.fits_utils.requests.get')
def test_download_from_s3_retries_on_empty_content(mock_get):
    archive_response = MagicMock()
    archive_response.status_code = 200
    archive_response.raise_for_status.return_value = None
    archive_response.json.return_value = {'url': 'https://s3.example.com/file.fits'}

    empty_s3_response = MagicMock()
    empty_s3_response.status_code = 200
    empty_s3_response.raise_for_status.return_value = None
    empty_s3_response.content = b''

    good_s3_response = MagicMock()
    good_s3_response.status_code = 200
    good_s3_response.raise_for_status.return_value = None
    good_s3_response.content = make_fits_bytes()

    mock_get.side_effect = [archive_response, empty_s3_response,
                            archive_response, good_s3_response]

    file_info = {'frameid': 42, 'filename': 'test.fits'}
    result = fits_utils.download_from_s3(file_info, FakeContext())

    assert mock_get.call_count == 4
    assert result.read(4) == b'SIMP'


@patch('banzai.utils.fits_utils.requests.get')
def test_download_from_s3_raises_after_all_retries_empty(mock_get):
    archive_response = MagicMock()
    archive_response.status_code = 200
    archive_response.raise_for_status.return_value = None
    archive_response.json.return_value = {'url': 'https://s3.example.com/file.fits'}

    empty_s3_response = MagicMock()
    empty_s3_response.status_code = 200
    empty_s3_response.raise_for_status.return_value = None
    empty_s3_response.content = b''

    mock_get.side_effect = [archive_response, empty_s3_response] * 4

    file_info = {'frameid': 42, 'filename': 'test.fits'}
    with pytest.raises(EOFError, match='Downloaded empty file from S3'):
        fits_utils.download_from_s3(file_info, FakeContext())


def test_table_to_fits():
    a = np.random.normal(size=100)
    b = np.random.normal(size=100)
    c = np.random.normal(size=100)

    t = Table([a, b, c], names=('a', 'b', 'c'))

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


def test_lossless_compression():
    uncompressed_error = np.random.uniform(size=(1001, 1003))

    hdu_list = fits.HDUList([fits.ImageHDU(data=np.zeros((1001, 1003)), header=Header({'EXTNAME': 'SCI'})),
                             fits.ImageHDU(data=uncompressed_error.copy(), header=Header({'EXTNAME': 'ERR'}))])

    fpacked_hdulist = fits_utils.pack(hdu_list, ['ERR'])
    # Write to a bytes stream to make sure there is no lazy compressing/caching
    # funny business
    buffer = io.BytesIO()
    fpacked_hdulist.writeto(buffer)
    buffer.seek(0)
    packed_hdu = fits.open(buffer, memmap=False)
    np.testing.assert_allclose(uncompressed_error, packed_hdu['ERR'].data, atol=1e-9)


def test_open_image():
    for fpacked in [True, False]:
        # Read an image with only a single extension
        # Read an images with a single extension and a BPM extension
        # Read an image with a single extension and a datacube
        # Read an image with multiple sci extensions
        pass
