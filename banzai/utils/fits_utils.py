import tempfile
import logging
from typing import Optional
from banzai import logs

import numpy as np
from astropy.io import fits
from astropy.coordinates import SkyCoord
from astropy import units

logger = logging.getLogger('banzai')

FITS_MANDATORY_KEYWORDS = ['SIMPLE', 'BITPIX', 'NAXIS', 'EXTEND', 'COMMENT', 'CHECKSUM', 'DATASUM']


def sanitizeheader(header):
    # Remove the mandatory keywords from a header so it can be copied to a new
    # image.
    header = header.copy()

    # Let the new data decide what these values should be
    for i in FITS_MANDATORY_KEYWORDS:
        if i in header.keys():
            header.pop(i)

    return header


def split_region_keyword(pixel_section):
    pixels = pixel_section.split(':')
    if int(pixels[1]) > int(pixels[0]):
        pixel_slice = slice(int(pixels[0]) - 1, int(pixels[1]), 1)
    else:
        if int(pixels[1]) == 1:
            pixel_slice = slice(int(pixels[0]) - 1, None, -1)
        else:
            pixel_slice = slice(int(pixels[0]) - 1, int(pixels[1]) - 2, -1)
    return pixel_slice


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


def get_configuration_mode(header):
    configuration_mode = header.get('CONFMODE', 'default')
    # If the configuration mode is not in the header, fallback to default to support legacy data
    if configuration_mode == 'N/A' or configuration_mode == 0 or configuration_mode.lower() == 'normal':
        configuration_mode = 'default'

    header['CONFMODE'] = configuration_mode
    return configuration_mode


def open_fits_file(filename: str):
    # TODO: deal with a datacube and munging
    # TODO: detect if AWS frame and stream the file in rather than just opening the file,
    # this is done using boto3 and a io.BytesIO() buffer
    with open(filename, 'rb') as f:
        hdu_list = fits.open(f, memmap=False)
        uncompressed_hdu_list = unpack(hdu_list)
        hdu_list.close()
        del hdu_list
    return uncompressed_hdu_list


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
                                               quantize_level=64,
                                               dither_seed=2048, quantize_method=1)
            hdulist.append(compressed_hdu)
        else:
            hdulist.append(hdu)
    return fits.HDUList(hdulist)
