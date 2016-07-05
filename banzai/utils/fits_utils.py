from __future__ import absolute_import, division, print_function, unicode_literals
from astropy.io import fits
from astropy.coordinates import SkyCoord
from astropy import units
import numpy as np
import os
import tempfile
from banzai.utils import date_utils
from banzai import logs

logger = logs.get_logger(__name__)

__author__ = 'cmccully'


def sanitizeheader(header):
    # Remove the mandatory keywords from a header so it can be copied to a new
    # image.
    header = header.copy()

    # Let the new data decide what these values should be
    for i in ['SIMPLE', 'BITPIX', 'BSCALE', 'BZERO']:
        if i in header.keys():
            header.pop(i)

    return header


def create_master_calibration_header(images):
    header = fits.Header()
    for h in images[0].header.keys():
        try:
            # Dump empty header keywords
            if len(h) > 0:
                header[h] = images[0].header[h]
        except ValueError as e:
            logging_tags = logs.image_config_to_tags(images[0], None)
            logs.add_tag(logging_tags, 'filename', images[0].filename)
            logger.error('Could not add keyword {0}'.format(h), extra=logging_tags)
            continue

    header = sanitizeheader(header)

    observation_dates = [image.dateobs for image in images]
    mean_dateobs = date_utils.mean_date(observation_dates)

    header['DATE-OBS'] = date_utils.date_obs_to_string(mean_dateobs)

    header.add_history("Images combined to create master calibration image:")
    for image in images:
        header.add_history(image.filename)
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


def table_to_fits(table):
    """
    Convert an astropy table to a fits binary table HDU
    :param table: astropy table
    :return: fits BinTableHDU
    """
    columns = [fits.Column(name=col.upper(), format=fits_formats(table[col].dtype),
                           array=table[col]) for col in table.colnames]
    return fits.BinTableHDU.from_columns(columns)


def parse_ra_dec(header):
    try:
        coord = SkyCoord(header.get('RA'), header.get('DEC'), unit=(units.hourangle, units.degree))
        ra = coord.ra.deg
        dec = coord.dec.deg
    except ValueError:
        # Fallback to CRVAL1 and CRVAL2
        try:
            coord = SkyCoord(header.get('CRVAl1'), header.get('CRVAL2'), unit=(units.degree, units.degree))
            ra = coord.ra.deg
            dec = coord.dec.deg
        except ValueError:
            # Fallback to Cat-RA and CAT-DEC
            try:
                coord = SkyCoord(header.get('CAT-RA'), header.get('CAT-DEC'), unit=(units.hourangle, units.degree))
                ra = coord.ra.deg
                dec = coord.dec.deg
            except ValueError as e:
                logger.error('Could not get initial pointing guess. {0}'.format(e),
                             extra={'tags': {'filename': header.get('ORIGNAME')}})
                ra, dec = np.nan, np.nan
    return ra, dec


def open_image(filename):
    base_filename = os.path.basename(filename)

    with tempfile.TemporaryDirectory() as tmpdirname:
        if filename[-3:] == '.fz':
            # Strip off the .fz
            output_filename = os.path.join(tmpdirname, base_filename)[:-3]
            os.system('funpack -O {0} {1}'.format(output_filename, filename))
            fits_filename = output_filename
        else:
            fits_filename = filename

        hdu = fits.open(fits_filename, 'readonly')
        data = hdu[0].data.astype(np.float32)
        header = hdu[0].header
        try:
            bpm = hdu['BPM'].data.astype(np.uint8)
        except KeyError:
            bpm = None
        hdu.close()

    return data, header, bpm


def fits_formats(dtype):
    """		
    Convert a numpy data type to a fits format code		
    :param dtype: dtype parameter from numpy array
    :return: string: Fits type code		
    """		
    format_code = ''		
    if 'bool' in dtype.name:
        format_code = 'L'		
    elif np.issubdtype(dtype, np.int16):
        format_code = 'I'		
    elif np.issubdtype(dtype, np.int32):
        format_code = 'J'		
    elif np.issubdtype(dtype, np.int64):
        format_code = 'K'		
    elif np.issubdtype(dtype, np.float32):
        format_code = 'E'		
    elif np.issubdtype(dtype, np.float64):
        format_code = 'D'		
    elif np.issubdtype(dtype, np.complex32):
        format_code = 'C'		
    elif np.issubdtype(dtype, np.complex64):
        format_code = 'M'		
    elif np.issubdtype(dtype, np.character):
        format_code = 'A'		
    return format_code		
