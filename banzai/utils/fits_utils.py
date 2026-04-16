import datetime
from typing import Optional
import requests
from collections.abc import Iterable
from collections import OrderedDict

from banzai import logs
from banzai.exceptions import FrameNotAvailableError

import numpy as np
from astropy.io import fits
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_not_exception_type
import io
import os
from banzai.metrics import add_telemetry_span_attribute, trace_function

logger = logs.get_logger()

FITS_MANDATORY_KEYWORDS = ['SIMPLE', 'BITPIX', 'NAXIS', 'EXTEND', 'COMMENT', 'CHECKSUM', 'DATASUM']


def sanitize_header(header):
    # Remove the mandatory keywords from a header so it can be copied to a new
    # image.
    header = header.copy()

    # Let the new data decide what these values should be
    for i in FITS_MANDATORY_KEYWORDS:
        if i in header.keys():
            header.pop(i)

    return header


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
            # Get the value of n in TTYPE
            n = k[5:]
            hdu.header['TCOMM{0}'.format(n)] = description
    return hdu


def get_primary_header(filename) -> Optional[fits.Header]:
    try:
        header = fits.getheader(filename, ext=0)
        for keyword in header:
            if keyword not in FITS_MANDATORY_KEYWORDS:
                return header
        return fits.getheader(filename, ext=1)

    except Exception:
        logger.error("Unable to open fits file: {}".format(logs.format_exception()), extra_tags={'filename': filename})
        return None


# Stop after 4 attempts, and back off exponentially with a minimum wait time of 4 seconds, and a maximum of 10.
# If it fails after 4 attempts, "reraise" the original exception back up to the caller.
# Don't retry FrameNotAvailableError - these are frames that don't exist in the archive.
@retry(
    wait=wait_exponential(multiplier=2, min=4, max=10),
    stop=stop_after_attempt(4),
    retry=retry_if_not_exception_type(FrameNotAvailableError),
    reraise=True
)
@trace_function("download_from_s3")
def download_from_s3(file_info, context, is_raw_frame=False):
    frame_id = file_info.get('frameid')
    add_telemetry_span_attribute('frame_id', frame_id)
    add_telemetry_span_attribute('frame_filename', file_info.get('filename'))
    logger.info(f"Downloading file {file_info.get('filename')} from archive. ID: {frame_id}.",
                extra_tags={'filename': file_info.get('filename'),
                            'attempt_number': download_from_s3.statistics['attempt_number']})

    if is_raw_frame:
        url = f'{context.RAW_DATA_FRAME_URL}/{frame_id}/?include_related_frames=false'
        archive_auth_header = context.RAW_DATA_AUTH_HEADER
    else:
        url = f'{context.ARCHIVE_FRAME_URL}/{frame_id}/?include_related_frames=false'
        archive_auth_header = context.ARCHIVE_AUTH_HEADER
    logger.info(f"Requesting archive URL {url} (auth header present: {bool(archive_auth_header)})")

    try:
        response = requests.get(url, headers=archive_auth_header, timeout=30)
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        message = 'Error downloading file from archive.'
        if int(response.status_code) == 429:
            message += ' Rate limited.'
        logger.error(
                message,
                extra_tags={
                    'filename': file_info.get('filename'),
                    'attempt_number': download_from_s3.statistics['attempt_number']
                }
            )
        raise
    except requests.exceptions.RequestException as e:
        message = "Archive download connection error."
        logger.error(
            f"{message} {e}",
            extra_tags={
                'filename': file_info.get('filename'),
                'attempt_number': download_from_s3.statistics['attempt_number']
            }
        )
        raise

    # Parse the JSON response
    response_data = response.json()

    # Check for "Not found" response - don't retry these
    if 'detail' in response_data and response_data['detail'] == 'Not found.':
        logger.warning(f"Frame {frame_id} not found in archive for {file_info.get('filename')}")
        raise FrameNotAvailableError(f"Frame {frame_id} not found in archive")

    buffer = io.BytesIO()
    try:
        response = requests.get(response_data['url'], timeout=60)
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        message = f'Error downloading file from S3. {response.status_code} {response.reason}.'
        if int(response.status_code) == 429:
            message += ' Rate limited.'
        logger.error(
            message,
            extra_tags={
                'filename': file_info.get('filename'),
                'attempt_number': download_from_s3.statistics['attempt_number']
            }
        )
        raise
    except requests.exceptions.RequestException as e:
        message = "S3 download connection error."
        logger.error(
            f"{message} {e}",
            extra_tags={
                'filename': file_info.get('filename'),
                'attempt_number': download_from_s3.statistics['attempt_number']
            }
        )
        raise
    downloaded_bytes = buffer.write(response.content)
    if downloaded_bytes == 0:
        logger.error(
            'Downloaded empty file from S3.',
            extra_tags={
                'filename': file_info.get('filename'),
                'attempt_number': download_from_s3.statistics['attempt_number']
            }
        )
        raise EOFError('Downloaded empty file from S3.')
    buffer.seek(0)
    add_telemetry_span_attribute('downloaded_bytes', downloaded_bytes)
    return buffer


def get_configuration_mode(header):
    configuration_mode = header.get('CONFMODE', 'default')
    # If the configuration mode is not in the header, fallback to default to support legacy data
    if configuration_mode == 'N/A' or configuration_mode == 0 or configuration_mode.lower() == 'normal':
        configuration_mode = 'default'

    header['CONFMODE'] = configuration_mode
    return configuration_mode


@retry(
    wait=wait_exponential(multiplier=2, min=4, max=10),
    stop=stop_after_attempt(4),
    reraise=True
)
def basename_search_in_archive(filename, dateobs, context, is_raw_frame=False):
    if is_raw_frame:
        url = f'{context.RAW_DATA_FRAME_URL}/'
        archive_auth_header = context.RAW_DATA_AUTH_HEADER
    else:
        url = f'{context.ARCHIVE_FRAME_URL}/'
        archive_auth_header = context.ARCHIVE_AUTH_HEADER

    basename = filename.replace('.fz', '').replace('.fits', '')
    start = (dateobs - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
    end = (dateobs + datetime.timedelta(days=3)).strftime('%Y-%m-%d')

    response = requests.get(url, headers=archive_auth_header,
                            params={'basename_exact': basename, 'start': start, 'end': end},
                            timeout=15)
    response.raise_for_status()
    frames = response.json()['results']
    if len(frames) > 0:
        frame_id = frames[0]['id']
    else:
        frame_id = None
    return frame_id


def open_fits_file(file_info, context, is_raw_frame=False):
    if file_info.get('data_buffer') is not None:
        filename = file_info.get('filename')
        frame_id = None
        buffer = file_info.get('data_buffer')
    elif file_info.get('path') is not None and os.path.exists(file_info.get('path')):
        buffer = open(file_info.get('path'), 'rb')
        filename = os.path.basename(file_info.get('path'))
        frame_id = None
    elif file_info.get('frameid') is not None:
        buffer = download_from_s3(file_info, context, is_raw_frame=is_raw_frame)
        filename = file_info.get('filename')
        frame_id = file_info.get('frameid')
    elif file_info.get('filename') is not None and file_info.get('dateobs') is not None:
        filename = file_info.get('filename')
        date_obs = file_info.get('dateobs')
        frame_id = basename_search_in_archive(filename, date_obs, context, is_raw_frame=is_raw_frame)
        if frame_id is None:
            raise ValueError(f'No frame with the filename {filename} exists in the archive near date-obs {date_obs}')
        file_info['frameid'] = frame_id
        buffer = download_from_s3(file_info, context, is_raw_frame=is_raw_frame)
    else:
        raise ValueError('This file does not exist and there is no frame id to get it from S3.')

    hdu_list = fits.open(buffer, memmap=False)
    uncompressed_hdu_list = fits.unpack(hdu_list)
    hdu_list.close()
    buffer.close()
    del hdu_list
    del buffer

    return uncompressed_hdu_list, filename, frame_id


def pack(uncompressed_hdulist: fits.HDUList, lossless_extensions: Iterable) -> fits.HDUList:
    quantize_levels = {ext: 1e9 for ext in lossless_extensions}
    return fits.pack(uncompressed_hdulist, extension_quantizations=quantize_levels)


def to_fits_image_extension(data, master_extension_name, extension_name, context, extension_version=None):
    extension_name = master_extension_name + extension_name
    for extname_to_condense in context.EXTENSION_NAMES_TO_CONDENSE:
        if extension_name == extname_to_condense:
            continue
        extension_name = extension_name.replace(extname_to_condense, '')
    header = fits.Header({'EXTNAME': extension_name})
    if extension_version is not None:
        header['EXTVER'] = extension_version
    return fits.ImageHDU(data=data, header=header)


def reorder_hdus(hdu_list: fits.HDUList, extensions: list):
    """
    Re-order HDUs in an HDUList before writing to disk
    :param hdu_list: Astropy fits.HDUList
    :param extensions: Ordered list of extensions by EXTNAME
    """
    if extensions is None:
        extensions = []
    extensions += [hdu.name for hdu in hdu_list]
    # Use an ordered dict to get unique elements
    extensions = list(OrderedDict.fromkeys(extensions))
    hdu_list.sort(key=lambda x: extensions.index(x.name))


def convert_extension_datatypes(hdu_list: fits.HDUList, extension_datatypes: dict):
    """
    Convert extensions' data types into desired form.
    :param hdu_list: FITS HDUList
    :param extension_datatypes: Dictionary of desired data types, keyed by extension name
    """
    for hdu in hdu_list:
        if hdu.name in extension_datatypes:
            hdu.data = hdu.data.astype(extension_datatypes[hdu.name])
