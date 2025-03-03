import datetime
from typing import Optional
import requests
from collections.abc import Iterable
from collections import OrderedDict

from banzai import logs

import numpy as np
from astropy.io import fits
from astropy.coordinates import SkyCoord
from astropy import units
from tenacity import retry, wait_exponential, stop_after_attempt
import io
import os

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
@retry(wait=wait_exponential(multiplier=2, min=4, max=10), stop=stop_after_attempt(4), reraise=True)
def download_from_s3(file_info, context, is_raw_frame=False):
    frame_id = file_info.get('frameid')
    logger.info(f"Downloading file {file_info.get('filename')} from archive. ID: {frame_id}.",
                extra_tags={'filename': file_info.get('filename'),
                            'attempt_number': download_from_s3.statistics['attempt_number']})

    if is_raw_frame:
        url = f'{context.RAW_DATA_FRAME_URL}/{frame_id}/?include_related_frames=false'
        archive_auth_header = context.RAW_DATA_AUTH_HEADER
    else:
        url = f'{context.ARCHIVE_FRAME_URL}/{frame_id}/?include_related_frames=false'
        archive_auth_header = context.ARCHIVE_AUTH_HEADER
    response = requests.get(url, headers=archive_auth_header)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        message = 'Error downloading file from archive.'
        if int(response.status_code) == 429:
            message =+ ' Rate limited.'
            logger.error(message, extra_tags={'filename': file_info.get('filename'),
                         'attempt_number': download_from_s3.statistics['attempt_number']})
            raise e
    buffer = io.BytesIO()
    buffer.write(requests.get(response.json()['url'], stream=True).content)
    buffer.seek(0)
    return buffer


def get_configuration_mode(header):
    configuration_mode = header.get('CONFMODE', 'default')
    # If the configuration mode is not in the header, fallback to default to support legacy data
    if configuration_mode == 'N/A' or configuration_mode == 0 or configuration_mode.lower() == 'normal':
        configuration_mode = 'default'

    header['CONFMODE'] = configuration_mode
    return configuration_mode


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
    frames = requests.get(url, headers=archive_auth_header,
                          params={'basename': basename, 'start': start, 'end': end}).json()['results']
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
    uncompressed_hdu_list = unpack(hdu_list)
    hdu_list.close()
    buffer.close()
    del hdu_list
    del buffer

    return uncompressed_hdu_list, filename, frame_id


def unpack(compressed_hdulist: fits.HDUList) -> fits.HDUList:
    # If the primary fits header only has the mandatory keywords, then we throw away that extension
    # and extension 1 gets moved to 0
    # Otherwise the primary HDU is kept
    move_1_to_0 = True
    for keyword in compressed_hdulist[0].header:
        if keyword not in FITS_MANDATORY_KEYWORDS:
            move_1_to_0 = False
            break
    if not move_1_to_0 or not isinstance(compressed_hdulist[1], fits.CompImageHDU):
        primary_hdu = fits.PrimaryHDU(data=compressed_hdulist[0].data, header=compressed_hdulist[0].header)
    else:
        data_type = str(compressed_hdulist[1].data.dtype)
        if 'int' == data_type[:3]:
            data_type = 'u' + data_type
            data = np.array(compressed_hdulist[1].data, getattr(np, data_type))
        else:
            data = compressed_hdulist[1].data
        primary_hdu = fits.PrimaryHDU(data=data, header=compressed_hdulist[1].header)
        if 'ZDITHER0' in primary_hdu.header:
            primary_hdu.header.pop('ZDITHER0')
    hdulist = [primary_hdu]
    if move_1_to_0:
        starting_extension = 2
    else:
        starting_extension = 1
    for hdu in compressed_hdulist[starting_extension:]:
        if isinstance(hdu, fits.CompImageHDU):
            if hdu.data is None:
                data = hdu.data
            else:
                data_type = str(hdu.data.dtype)
                if 'int' == data_type[:3]:
                    data_type = getattr(np, 'u' + data_type)
                    data = np.array(hdu.data, data_type)
                else:
                    data = np.array(hdu.data, hdu.data.dtype)
            hdulist.append(fits.ImageHDU(data=data, header=hdu.header))
        elif isinstance(hdu, fits.BinTableHDU):
            hdulist.append(fits.BinTableHDU(data=hdu.data, header=hdu.header))
        else:
            hdulist.append(fits.ImageHDU(data=hdu.data, header=hdu.header))
    return fits.HDUList(hdulist)


def pack(uncompressed_hdulist: fits.HDUList, lossless_extensions: Iterable) -> fits.HDUList:
    if uncompressed_hdulist[0].data is None:
        primary_hdu = fits.PrimaryHDU(header=uncompressed_hdulist[0].header)
        hdulist = [primary_hdu]
    else:
        primary_hdu = fits.PrimaryHDU()
        if uncompressed_hdulist[0].header['EXTNAME'] in lossless_extensions:
            quantize_level = 1e9
        else:
            quantize_level = 64
        if uncompressed_hdulist[0].data is None:
            data = None
        else:
            data = np.ascontiguousarray(uncompressed_hdulist[0].data)
        compressed_hdu = fits.CompImageHDU(data=data,
                                           header=uncompressed_hdulist[0].header, quantize_level=quantize_level,
                                           quantize_method=1)
        hdulist = [primary_hdu, compressed_hdu]

    for hdu in uncompressed_hdulist[1:]:
        if isinstance(hdu, fits.ImageHDU):
            if hdu.header['EXTNAME'] in lossless_extensions:
                quantize_level = 1e9
            else:
                quantize_level = 64
            if hdu.data is None:
                data = None
            else:
                data = np.ascontiguousarray(hdu.data)
            compressed_hdu = fits.CompImageHDU(data=data, header=hdu.header,
                                               quantize_level=quantize_level, quantize_method=1)
            hdulist.append(compressed_hdu)
        else:
            hdulist.append(hdu)
    return fits.HDUList(hdulist)


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
