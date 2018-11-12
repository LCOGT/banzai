import os
import tempfile
import logging

import numpy as np
from astropy.io import fits
from astropy.coordinates import SkyCoord
from astropy import units

from banzai.utils import date_utils

logger = logging.getLogger(__name__)


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
                header[h] = (images[0].header[h], images[0].header.comments[h])
        except ValueError as e:
            logger.error('Could not add keyword {0}'.format(h), image=images[0])
            continue

    header = sanitizeheader(header)

    observation_dates = [image.dateobs for image in images]
    mean_dateobs = date_utils.mean_date(observation_dates)

    header['DATE-OBS'] = (date_utils.date_obs_to_string(mean_dateobs), '[UTC] Mean observation start time')

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
        coord = SkyCoord(header.get('RA'), header.get('DEC'), unit=(units.hourangle, units.degree))
        ra = coord.ra.deg
        dec = coord.dec.deg
    except (ValueError, TypeError):
        # Fallback to CRVAL1 and CRVAL2
        try:
            coord = SkyCoord(header.get('CRVAl1'), header.get('CRVAL2'), unit=(units.degree, units.degree))
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


def open_fits_file(filename):
    """
    Load a fits file

    Parameters
    ----------
    filename: str
              File name/path to open

    Returns
    -------
    hdulist: astropy.io.fits

    Notes
    -----
    This is a wrapper to astropy.io.fits.open but funpacks the file first.
    """
    base_filename, file_extension = os.path.splitext(os.path.basename(filename))
    if file_extension == '.fz':
        with tempfile.TemporaryDirectory() as tmpdirname:
            output_filename = os.path.join(tmpdirname, base_filename)
            os.system('funpack -O {0} {1}'.format(output_filename, filename))
            hdulist = fits.open(output_filename, 'readonly')
    else:
        hdulist = fits.open(filename, 'readonly')

    return hdulist


def open_image(filename):
    """
    Load an image from a FITS file

    Parameters
    ----------
    filename: str
              Full path of the file to open

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
    hdulist = open_fits_file(filename)

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
