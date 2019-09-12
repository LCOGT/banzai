import os
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


def init_hdu():
    file_handle = tempfile.NamedTemporaryFile()
    return fits.open(file_handle, memmap=True)


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


def open_fits_file(filename: str):
    # TODO: deal with a datacube and munging
    # TODO: detect if AWS frame and stream the file in rather than just opening the file,
    # this is done using boto3 and a io.BytesIO() buffer
    with open(filename, 'rb') as f:
        hdu_list = fits.open(f)
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
        else:
            hdulist.append(hdu)
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


def _writeto(self, filepath, fpack=False):
    logger.info('Writing file to {filepath}'.format(filepath=filepath), image=self)
    hdu_list = self._get_hdu_list()
    base_filename = os.path.basename(filepath).split('.fz')[0]
    with tempfile.TemporaryDirectory() as temp_directory:
        hdu_list.writeto(os.path.join(temp_directory, base_filename), overwrite=True, output_verify='fix+warn')
        hdu_list.close()
        if fpack:
            if os.path.exists(filepath):
                os.remove(filepath)
            command = 'fpack -q 64 {temp_directory}/{basename}'
            os.system(command.format(temp_directory=temp_directory, basename=base_filename))
            base_filename += '.fz'
        shutil.move(os.path.join(temp_directory, base_filename), filepath)


def _get_hdu_list(self):
    image_hdu = fits.PrimaryHDU(self.data.astype(np.float32), header=self.header)
    image_hdu.header['BITPIX'] = -32
    image_hdu.header['BSCALE'] = 1.0
    image_hdu.header['BZERO'] = 0.0
    image_hdu.header['SIMPLE'] = True
    image_hdu.header['EXTEND'] = True
    image_hdu.name = 'SCI'

    hdu_list = [image_hdu]
    hdu_list = self._add_data_tables_to_hdu_list(hdu_list)
    hdu_list = self._add_bpm_to_hdu_list(hdu_list)
    fits_hdu_list = fits.HDUList(hdu_list)
    try:
        fits_hdu_list.verify(option='exception')
    except fits.VerifyError as fits_error:
        logger.warning('Error in FITS Verification. {0}. Attempting fix.'.format(fits_error), image=self)
        try:
            fits_hdu_list.verify(option='silentfix+exception')
        except fits.VerifyError as fix_attempt_error:
            logger.error('Could not repair FITS header. {0}'.format(fix_attempt_error), image=self)
    return fits.HDUList(hdu_list)
