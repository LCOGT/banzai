import logging
from typing import Optional, List
import requests

from banzai import logs
from banzai.context import Context

import numpy as np
from astropy.io import fits
from astropy.coordinates import SkyCoord
from astropy import units
from tenacity import retry, wait_exponential, stop_after_attempt
import io
import os

logger = logging.getLogger('banzai')

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
                            'attempt_number': download_from_s3.retry.statistics['attempt_number']})

    if is_raw_frame:
        url = f'{context.RAW_DATA_FRAME_URL}/{frame_id}'
        archive_auth_token = context.RAW_DATA_AUTH_TOKEN
    else:
        url = f'{context.ARCHIVE_FRAME_URL}/{frame_id}'
        archive_auth_token = context.ARCHIVE_AUTH_TOKEN
    response = requests.get(url, headers=archive_auth_token).json()
    buffer = io.BytesIO()
    buffer.write(requests.get(response['url'], stream=True).content)
    buffer.seek(0)
    return buffer


def get_configuration_mode(header):
    configuration_mode = header.get('CONFMODE', 'default')
    # If the configuration mode is not in the header, fallback to default to support legacy data
    if configuration_mode == 'N/A' or configuration_mode == 0 or configuration_mode.lower() == 'normal':
        configuration_mode = 'default'

    header['CONFMODE'] = configuration_mode
    return configuration_mode


def open_fits_file(file_info, context, is_raw_frame=False):
    if file_info.get('path') is not None and os.path.exists(file_info.get('path')):
        buffer = open(file_info.get('path'), 'rb')
        filename = os.path.basename(file_info.get('path'))
        frame_id = None
    elif file_info.get('frameid') is not None:
        buffer = download_from_s3(file_info, context, is_raw_frame=is_raw_frame)
        filename = file_info.get('filename')
        frame_id = file_info.get('frameid')
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
        primary_hdu = fits.PrimaryHDU(data=None, header=compressed_hdulist[0].header)
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
            data_type = str(hdu.data.dtype)
            if 'int' == data_type[:3]:
                data_type = getattr(np, 'u' + data_type)
                data = np.array(hdu.data, data_type)
            else:
                data = np.array(hdu.data, hdu.data.dtype)
            header = hdu.header
            hdulist.append(fits.ImageHDU(data=data, header=header))
        elif isinstance(hdu, fits.BinTableHDU):
            hdulist.append(fits.BinTableHDU(data=hdu.data, header=hdu.header))
        else:
            hdulist.append(fits.ImageHDU(data=hdu.data, header=hdu.header))
    return fits.HDUList(hdulist)


def pack(uncompressed_hdulist: fits.HDUList) -> fits.HDUList:
    if uncompressed_hdulist[0].data is None:
        primary_hdu = fits.PrimaryHDU(header=uncompressed_hdulist[0].header)
        hdulist = [primary_hdu]
    else:
        primary_hdu = fits.PrimaryHDU()
        compressed_hdu = fits.CompImageHDU(data=np.ascontiguousarray(uncompressed_hdulist[0].data),
                                           header=uncompressed_hdulist[0].header, quantize_level=64,
                                           dither_seed=2048, quantize_method=1)
        hdulist = [primary_hdu, compressed_hdu]

    for hdu in uncompressed_hdulist[1:]:
        if isinstance(hdu, fits.ImageHDU):
            compressed_hdu = fits.CompImageHDU(data=np.ascontiguousarray(hdu.data), header=hdu.header,
                                               quantize_level=64, quantize_method=1)
            hdulist.append(compressed_hdu)
        else:
            hdulist.append(hdu)
    return fits.HDUList(hdulist)


def to_fits_image_extension(data, master_extension_name, extension_name, context, extension_version=None):
    extension_name = master_extension_name + extension_name
    for extname_to_condense in context.EXTENSION_NAMES_TO_CONDENSE:
        extension_name = extension_name.replace(extname_to_condense, '')
    header = fits.Header({'EXTNAME': extension_name})
    if extension_version is not None:
        header['EXTVER'] = extension_version
    return fits.ImageHDU(data=data, header=header)


def reorder_hdus(hdu_list: fits.HDUList,  obstype: str, ordering_dict: dict):
    """
    Re-order HDUs in an HDUList before writing to disk
    :param hdu_list: Astropy fits.HDUList
    :param obstype: Observation type from header
    :param ordering_dict: A dictionary, keyed by OBSTYPE with the desired ordering of
    extensions by EXTNAME
    """
    for idx, extension_name in enumerate(ordering_dict.get(obstype)):
        if hdu_list[idx].name != extension_name:
            hdu = hdu_list[extension_name]
            hdu_list.remove(hdu)
            hdu_list.insert(idx, hdu)
