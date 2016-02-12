from __future__ import absolute_import, print_function, division
from astropy.io import fits
import numpy as np

__author__ = 'cmccully'

def sanitizeheader(header):
    # Remove the mandatory keywords from a header so it can be copied to a new
    # image.
    header = header.copy()

    # Let the new data decide what these values should be
    for i in ['SIMPLE', 'BITPIX', 'BSCALE', 'BZERO']:
        if i in header.keys():
            header.pop(i)

    if 'NAXIS' in header.keys():
        naxis = header.pop('NAXIS')
        for i in range(naxis):
            header.pop('NAXIS%i' % (i + 1))

    return header


def split_slice(pixel_section):
    pixels = pixel_section.split(':')
    return slice(int(pixels[0]) - 1, int(pixels[1]))


def parse_region_keyword(keyword_value):
    """
    Convert a header keyword of the form [x1:x2],[y1:y2] into index slices
    :param keyword_value: Header keyword string
    :return: x, y index slices
    """

    if keyword_value.lower() == 'unknown':
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


def fits_formats(format):
    """
    Convert a numpy data type to a fits format code
    :param format: dtype parameter from numpy array
    :return: string: Fits type code
    """
    format_code = ''
    if np.issubdtype(format, np.bool):
        format_code = 'L'
    elif np.issubdtype(format, np.int16):
        format_code = 'I'
    elif np.issubdtype(format, np.int32):
        format_code = 'J'
    elif np.issubdtype(format, np.int64):
        format_code = 'K'
    elif np.issubdtype(format, np.float32):
        format_code = 'E'
    elif np.issubdtype(format, np.float64):
        format_code = 'D'
    elif np.issubdtype(format, np.complex32):
        format_code = 'C'
    elif np.issubdtype(format, np.complex64):
        format_code = 'M'
    elif np.issubdtype(format, np.character):
        format_code = 'A'
    return format_code


def table_to_fits(table):
    """
    Convert an astropy table to a fits binary table HDU
    :param table: astropy table
    :return: fits BinTableHDU
    """
    columns = [fits.Column(name=col.upper(), format=fits_formats(table[col].dtype),
                           array=table[col]) for col in table.colnames]
    return fits.BinTableHDU.from_columns(columns)
