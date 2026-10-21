import os
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


# --- uncompressed sibling tests ---

def write_compressed_master(path, value):
    header = fits.Header({'OBSTYPE': 'BIAS', 'EXTNAME': 'SCI', 'GAIN': 1.0, 'ISMASTER': True})
    fits.HDUList([fits.PrimaryHDU(),
                  fits.CompImageHDU(np.full((4, 6), value, dtype=np.float32), header, name='SCI'),
                  fits.CompImageHDU(np.ones((4, 6), dtype=np.float32) * 0.1, name='ERR'),
                  fits.CompImageHDU(np.zeros((4, 6), dtype=np.uint8), name='BPM')]).writeto(path, overwrite=True)


def write_uncompressed_sibling(path, value):
    """An uncompressed file in the layout the download worker writes beside a .fz master."""
    header = fits.Header({'OBSTYPE': 'BIAS', 'EXTNAME': 'SCI', 'GAIN': 1.0, 'ISMASTER': True})
    fits.HDUList([fits.PrimaryHDU(np.full((4, 6), value, dtype=np.float32), header),
                  fits.ImageHDU(np.ones((4, 6), dtype=np.float32) * 0.1, name='ERR'),
                  fits.ImageHDU(np.zeros((4, 6), dtype=np.uint8), name='BPM')]).writeto(path, overwrite=True)


def assert_same_hdus(actual, expected):
    assert len(actual) == len(expected)
    for a, e in zip(actual, expected):
        assert type(a) is type(e)
        assert a.name == e.name
        assert a.data.dtype == e.data.dtype
        assert a.data.dtype.isnative
        np.testing.assert_array_equal(a.data, e.data)
        assert a.header.tostring() == e.header.tostring()


def test_uncompressed_sibling_path():
    assert fits_utils.uncompressed_sibling('/cals/master.fits.fz') == '/cals/master.fits'
    assert fits_utils.uncompressed_sibling('/cals/master.fits') is None


def test_open_fits_file_prefers_uncompressed_sibling(tmp_path):
    path = str(tmp_path / 'master.fits.fz')
    write_compressed_master(path, 1.0)
    expected, _, _ = fits_utils.open_fits_file({'path': path}, FakeContext())
    with fits.open(path) as compressed:
        fits_utils.unpack(compressed).writeto(fits_utils.uncompressed_sibling(path))

    with patch('banzai.utils.fits_utils.unpack', wraps=fits_utils.unpack) as unpack:
        hdus, filename, frame_id = fits_utils.open_fits_file({'path': path}, FakeContext())

    unpack.assert_not_called()
    assert (filename, frame_id) == ('master.fits.fz', None)
    assert_same_hdus(hdus, expected)
    assert hdus[0].data.flags.writeable
    hdus[0].data[0, 0] = -1  # private, in-memory arrays: the file is closed and untouched
    with fits.open(fits_utils.uncompressed_sibling(path)) as sibling:
        assert sibling[0].data[0, 0] == 1.0


def test_open_fits_file_ignores_sibling_for_raw_frames(tmp_path):
    path = str(tmp_path / 'raw.fits.fz')
    write_compressed_master(path, 1.0)
    write_uncompressed_sibling(fits_utils.uncompressed_sibling(path), 2.0)

    hdus, _, _ = fits_utils.open_fits_file({'path': path}, FakeContext(), is_raw_frame=True)

    np.testing.assert_array_equal(hdus[0].data, 1.0)


def test_open_fits_file_falls_back_on_unreadable_sibling(tmp_path, caplog):
    path = str(tmp_path / 'master.fits.fz')
    write_compressed_master(path, 1.0)
    with open(fits_utils.uncompressed_sibling(path), 'wb') as corrupt:
        corrupt.write(b'not a fits file')

    hdus, _, _ = fits_utils.open_fits_file({'path': path}, FakeContext())

    np.testing.assert_array_equal(hdus[0].data, 1.0)
    assert 'uncompressed' in caplog.text.lower()
