import os
import tempfile
import logging
import copy
import requests

from banzai import logs

import numpy as np
from astropy.io import fits
from astropy.coordinates import SkyCoord
from astropy import units
from tenacity import retry, wait_exponential, stop_after_attempt

logger = logging.getLogger('banzai')


def sanitizeheader(header):
    # Remove the mandatory keywords from a header so it can be copied to a new
    # image.
    header = header.copy()

    # Let the new data decide what these values should be
    for i in ['SIMPLE', 'BITPIX', 'BSCALE', 'BZERO']:
        if i in header.keys():
            header.pop(i)

    return header


def split_slice(pixel_section):
    pixels = pixel_section.split(':')
    if int(pixels[1]) > int(pixels[0]):
        pixel_slice = slice(int(pixels[0]) - 1, int(pixels[1]), 1)
    else:
        if int(pixels[1]) == 1:
            pixel_slice = slice(int(pixels[0]) - 1, None, -1)
        else:
            pixel_slice = slice(int(pixels[0]) - 1, int(pixels[1]) - 2, -1)
    return pixel_slice


def parse_region_keyword(keyword_value):
    """
    Convert a header keyword of the form [x1:x2],[y1:y2] into index slices
    :param keyword_value: Header keyword string
    :return: x, y index slices
    """
    if not keyword_value:
        pixel_slices = None
    elif keyword_value.lower() == 'unknown':
        pixel_slices = None
    elif keyword_value.lower() == 'n/a':
        pixel_slices = None
    else:
        # Strip off the brackets and split the coordinates
        pixel_sections = keyword_value[1:-1].split(',')
        x_slice = split_slice(pixel_sections[0])
        y_slice = split_slice(pixel_sections[1])
        pixel_slices = (y_slice, x_slice)
    return pixel_slices


def table_to_fits(table):
    """
    Convert an astropy table to a fits binary table HDU
    :param table: astropy table
    :return: fits BinTableHDU
    """
    hdu = fits.BinTableHDU(table)
    # Put in the description keywords
    for k in hdu.header.keys():
        if 'TTYPE' in k:
            column_name = hdu.header[k].lower()
            description = table[column_name].description
            hdu.header[k] = (column_name.upper(), description)
            # Get the value of n in TTYPEn
            n = k[5:]
            hdu.header['TCOMM{0}'.format(n)] = description
    return hdu


def parse_ra_dec(header):
    try:
        coord = SkyCoord(header.get('CRVAl1'), header.get('CRVAL2'), unit=(units.degree, units.degree))
        ra = coord.ra.deg
        dec = coord.dec.deg
    except (ValueError, TypeError):
        # Fallback to RA and DEC
        try:
            coord = SkyCoord(header.get('RA'), header.get('DEC'), unit=(units.hourangle, units.degree))
            ra = coord.ra.deg
            dec = coord.dec.deg
        except (ValueError, TypeError):
            # Fallback to Cat-RA and CAT-DEC
            try:
                coord = SkyCoord(header.get('CAT-RA'), header.get('CAT-DEC'), unit=(units.hourangle, units.degree))
                ra = coord.ra.deg
                dec = coord.dec.deg
            except (ValueError, TypeError) as e:
                logger.error('Could not get initial pointing guess. {0}'.format(e),
                             extra_tags={'filename': header.get('ORIGNAME')})
                ra, dec = np.nan, np.nan
    return ra, dec


def open_fits_file(file_info, runtime_context):
    """
    Load a fits file

    Parameters
    ----------
    :param file_info: Queue message body: dict
    :param runtime_context: Context object with runtime environment info

    Returns
    -------
    hdulist: astropy.io.fits

    Notes
    -----
    This is a wrapper to astropy.io.fits.open but funpacks the file first.
    """
    base_filename, file_extension = os.path.splitext(get_filename_from_info(file_info))
    path_to_file = get_local_path_from_info(file_info, runtime_context)

    need_to_download = False
    if not os.path.isfile(path_to_file):
        need_to_download = True

    if file_extension == '.fz':
        with tempfile.TemporaryDirectory() as tmpdirname:
            output_filepath = os.path.join(tmpdirname, base_filename)
            if need_to_download:
                downloaded_filepath = download_from_s3(file_info, tmpdirname, runtime_context)
                os.system('funpack -O {0} {1}'.format(output_filepath, downloaded_filepath))
            else:
                os.system('funpack -O {0} {1}'.format(output_filepath, path_to_file))

            hdulist = fits.open(output_filepath, 'readonly')
            hdulist_copy = copy.deepcopy(hdulist)
            hdulist.close()
    else:
        hdulist = fits.open(path_to_file, 'readonly')
        hdulist_copy = copy.deepcopy(hdulist)
        hdulist.close()
    return hdulist_copy


def open_image(file_info, runtime_context):
    """
    Load an image from a FITS file

    Parameters
    ----------
    :param file_info: Queue message body: dict
    :param runtime_context: Context object with runtime environment info

    Returns
    -------
    data: numpy array
          image data; will have 3 dimensions if the file was either multi-extension or
          a datacube
    header: astropy.io.fits.Header
            Header from the primary extension
    bpm: numpy array
         Array of bad pixel mask values if the BPM extension exists. None otherwise.
    extension_headers: list of astropy.io.fits.Header
                       List of headers from other SCI extensions that are not the
                       primary extension

    Notes
    -----
    The file can be either compressed or not. If there are multiple extensions,
    e.g. Sinistros, the extensions should be (SCI, 1), (SCI, 2), ...
    Sinsitro frames that were taken as datacubes will be munged later so that the
    output images are consistent
    """
    hdulist = open_fits_file(file_info, runtime_context)

    # Get the main header
    header = hdulist[0].header

    # Check for multi-extension fits
    extension_headers = []
    sci_extensions = get_extensions_by_name(hdulist, 'SCI')
    if len(sci_extensions) > 1:
        data = np.zeros((len(sci_extensions), sci_extensions[0].data.shape[0],
                         sci_extensions[0].data.shape[1]), dtype=np.float32)
        for i, hdu in enumerate(sci_extensions):
            data[i, :, :] = hdu.data[:, :]
            extension_headers.append(hdu.header)
    elif len(sci_extensions) == 1:
        data = sci_extensions[0].data.astype(np.float32)
    else:
        data = hdulist[0].data.astype(np.float32)

    try:
        bpm = hdulist['BPM'].data.astype(np.uint8)
    except KeyError:
        bpm = None

    return data, header, bpm, extension_headers


def get_basename(path):
    basename = None
    if path is not None:
        filename = os.path.basename(path)
        if filename.find('.') > 0:
            basename = filename[:filename.index('.')]
        else:
            basename = filename
    return basename


# Stop after 4 attempts, and back off exponentially with a minimum wait time of 4 seconds, and a maximum of 10.
# If it fails after 4 attempts, "reraise" the original exception back up to the caller.
@retry(wait=wait_exponential(multiplier=2, min=4, max=10), stop=stop_after_attempt(4), reraise=True)
def download_from_s3(file_info, output_directory, runtime_context):
    frame_id = file_info.get('frameid')
    filename = get_filename_from_info(file_info)

    logger.info(f"Downloading file {file_info.get('filename')} from archive. ID: {frame_id}.",
                extra_tags={'filename': file_info.get('filename'),
                            'attempt_number': download_from_s3.retry.statistics['attempt_number']})

    if frame_id is not None:
        url = f'{runtime_context.ARCHIVE_FRAME_URL}/{frame_id}'
        response = requests.get(url, headers=runtime_context.ARCHIVE_AUTH_TOKEN).json()
        path = os.path.join(output_directory, response['filename'])
        with open(path, 'wb') as f:
            f.write(requests.get(response['url'], stream=True).content)
    else:
        basename = get_basename(filename)
        url = f'{runtime_context.ARCHIVE_FRAME_URL}/?basename={basename}'
        response = requests.get(url, headers=runtime_context.ARCHIVE_AUTH_TOKEN).json()
        path = os.path.join(output_directory, response['results'][0]['filename'])
        with open(path, 'wb') as f:
            f.write(requests.get(response['results'][0]['url'], stream=True).content)

    return path


def get_extensions_by_name(fits_hdulist, name):
    """
    Get a list of the science extensions from a multi-extension fits file (HDU list)

    Parameters
    ----------
    fits_hdulist: HDUList
                  input fits HDUList to search for SCI extensions

    name: str
          Extension name to collect, e.g. SCI

    Returns
    -------
    HDUList: an HDUList object with only the SCI extensions
    """
    # The following of using False is just an awful convention and will probably be
    # deprecated at some point
    extension_info = fits_hdulist.info(False)
    return fits.HDUList([fits_hdulist[ext[0]] for ext in extension_info if ext[1] == name])


def get_configuration_mode(header):
    configuration_mode = header.get('CONFMODE', 'default')
    # If the configuration mode is not in the header, fallback to default to support legacy data
    if (
            configuration_mode == 'N/A' or
            configuration_mode == 0 or
            configuration_mode.lower() == 'normal'
    ):
        configuration_mode = 'default'

    header['CONFMODE'] = configuration_mode
    return configuration_mode


def get_primary_header(file_info, runtime_context):
    try:
        hdulist = open_fits_file(file_info, runtime_context)
        return hdulist[0].header
    except Exception:
        logger.error("Unable to open fits file: {}".format(logs.format_exception()),
                     extra_tags={'filename': get_filename_from_info(file_info)})
        return None


def get_filename_from_info(file_info):
    """
    Get a filename from a queue message
    :param file_info: Queue message body: dict
    :return: filename : str

    When running using a /archive mount, BANZAI listens on the fits_queue, which contains a
    path to an image on the archive machine. When running using AWS and s3, we listen to archived_fits
    which contains a complete dictionary of image parameters, one of which is a filename including extension.
    """
    path = file_info.get('path')
    if path is None:
        path = file_info.get('filename')
    return os.path.basename(path)


def get_local_path_from_info(file_info, runtime_context):
    """
    Given a message from an LCO fits queue, determine where the image would
    be stored locally by the pipeline.
    :param file_info: Queue message body: dict
    :param runtime_context: Context object with runtime environment info
    :return: filepath: str
    """
    if is_s3_queue_message(file_info):
        # archived_fits contains a dictionary of image attributes and header values
        path = os.path.join(runtime_context.processed_path, file_info.get('SITEID'),
                            file_info.get('INSTRUME'), file_info.get('DAY-OBS'))

        if file_info.get('RLEVEL') == 0:
            path = os.path.join(path, 'raw')
        elif file_info.get('RLEVEL') == 91:
            path = os.path.join(path, 'processed')

        path = os.path.join(path, file_info.get('filename'))
    else:
        # fits_queue contains paths to images on /archive
        path = file_info.get('path')

    return path


def is_s3_queue_message(file_info):
    """
    Determine if we are reading from s3 based on the contents of the
    message on the queue
    :param file_info: Queue message body: dict
    :return: True if we should read from s3, else False
    """
    s3_queue_message = False
    if file_info.get('path') is None:
        s3_queue_message = True

    return s3_queue_message
