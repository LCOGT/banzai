import hashlib
import os
from time import sleep

import numpy as np

from ocs_ingester import ingester
from ocs_ingester.exceptions import RetryError, DoNotRetryError, BackoffRetryError, NonFatalDoNotRetryError
from astropy.io.fits.hdu.hdulist import HDUList
from astropy.io.fits.hdu.compressed.compressed import CompImageHDU
from astropy.io.fits.hdu.image import ImageHDU, PrimaryHDU

from banzai.utils import import_utils
from banzai.logs import get_logger

logger = get_logger()

FITS_MANDATORY_KEYWORDS = [
    "SIMPLE",
    "BITPIX",
    "NAXIS",
    "EXTEND",
    "COMMENT",
    "CHECKSUM",
    "DATASUM",
]

def get_processed_path(base_path, site, camera, epoch):
    return os.path.join(base_path, site, camera, epoch, 'processed')


def make_jpg_filenames(smartstack_filename):
    """Build small and large JPEG filenames for a smartstack FITS filename.

    Parameters
    ----------
    smartstack_filename : str
        Smartstack FITS filename.

    Returns
    -------
    tuple
        Tuple of ``(small_thumbnail, large_thumbnail)`` JPEG filenames, matching
        the ``small_thumbnail``/``large_thumbnail`` keys in the shipper message.
    """
    if smartstack_filename.endswith('.fits.fz'):
        base = smartstack_filename[:-len('.fits.fz')]
    elif smartstack_filename.endswith('.fits'):
        base = smartstack_filename[:-len('.fits')]
    else:
        base = smartstack_filename

    return f'{base}-small_thumbnail.jpg', f'{base}-large_thumbnail.jpg'


def post_to_ingester(file_object, image, output_filename, meta=None):
    logger.info('Posting file to the archive', image=image)
    retry = True
    try_counter = 1
    ingester_response = {}
    while retry:
        try:
            ingester_response = ingester.upload_file_and_ingest_to_archive(file_object, path=output_filename,
                                                                           file_metadata=meta)
            logger.debug(f"Ingester response: {ingester_response}", image=image)
            retry = False
        except DoNotRetryError as exc:
            logger.warning('Exception occured: {0}. Aborting.'.format(exc), image=image)
            retry = False
        except NonFatalDoNotRetryError as exc:
            logger.debug('Non-fatal Exception occured: {0}. Aborting.'.format(exc), image=image)
            retry = False
        except RetryError as exc:
            logger.debug('Retry Exception occured: {0}. Retrying.'.format(exc), image=image)
            retry = True
            try_counter += 1
        except BackoffRetryError as exc:
            logger.debug('BackoffRetry Exception occured: {0}. Retrying.'.format(exc), image=image)
            if try_counter > 5:
                logger.warning('Giving up because we tried too many times.', image=image)
                retry = False
            else:
                sleep(5 ** try_counter)
                retry = True
                try_counter += 1
    return ingester_response


def get_md5(filepath):
    with open(filepath, 'rb') as file:
        md5 = hashlib.md5(file.read()).hexdigest()
    return md5


def ccdsum_to_filename(image):
    if image.binning is None:
        ccdsum_str = ''
    else:
        ccdsum_str = 'bin{ccdsum}'.format(ccdsum=str(image.binning[0]) + 'x' + str(image.binning[1]))
    return ccdsum_str


def filter_to_filename(image):
    return str(image.filter)


def config_to_filename(image):
    filename = str(image.configuration_mode)
    filename = filename.replace('full_frame', '')
    filename = filename.replace('default', '')
    filename = filename.replace('central_2k_2x2', 'center')
    return filename


def telescope_to_filename(image):
    return image.meta.get('TELESCOP', '').replace('-', '')


def make_calibration_filename_function(calibration_type, context):
    def get_calibration_filename(image):
        telescope_filename_function = import_utils.import_attribute(context.TELESCOPE_FILENAME_FUNCTION)
        name_components = {'site': image.instrument.site, 'telescop': telescope_filename_function(image),
                           'camera': image.instrument.camera, 'epoch': image.epoch,
                           'cal_type': calibration_type.lower()}
        cal_file = '{site}{telescop}-{camera}-{epoch}-{cal_type}'.format(**name_components)
        for function_name in context.CALIBRATION_FILENAME_FUNCTIONS[calibration_type]:
            filename_function = import_utils.import_attribute(function_name)
            filename_part = filename_function(image)
            if len(filename_part) > 0:
                cal_file += '-{}'.format(filename_part)
        cal_file += '.fits'
        return cal_file

    return get_calibration_filename

def unpack(compressed_hdulist: HDUList) -> HDUList:
    """
    Unpack a compressed FITS HDUList in an equivalent way to funpack from
    the cfitsio library.

    Parameters
    ----------
    compressed_hdulist : `HDUList`
        The compressed FITS HDUList to be unpacked.

    Returns
    -------
    uncompressed_hdulist : `HDUList`
        The uncompressed FITS HDUList.

    Notes
    -----
    If the primary HDU of the uncompressed HDUList is an image HDU, then
    fpacked file will have a primary header with only the manadatory header
    keywords for a FITS file. In this case, we remove this and make the original primary HDU, the uncompressed HDU so the newly uncompressed
    file matches the original. If there are other keywords, in the header
    of the compressed file, then the next HDU was not the primary and we
    decompress accordingly.
    """
    # If the primary fits header only has the mandatory keywords, then we throw away that extension
    # and extension 1 gets moved to 0
    # Otherwise, the primary HDU is kept
    move_1_to_0 = True
    for keyword in compressed_hdulist[0].header:
        if keyword not in FITS_MANDATORY_KEYWORDS:
            move_1_to_0 = False
            break
    if not move_1_to_0 or not isinstance(compressed_hdulist[1], CompImageHDU):
        primary_hdu = PrimaryHDU(
            data=compressed_hdulist[0].data, header=compressed_hdulist[0].header
        )
    else:
        data = compressed_hdulist[1].data
        primary_hdu = PrimaryHDU(data=data, header=compressed_hdulist[1].header)
    hdulist = [primary_hdu]
    if move_1_to_0:
        starting_extension = 2
    else:
        starting_extension = 1
    for hdu in compressed_hdulist[starting_extension:]:
        if isinstance(hdu, CompImageHDU):
            # If the data has been lazy loaded, we need to actualize the data
            # into an array.
            if hdu.data is None:
                data = hdu.data
            else:
                data = np.array(hdu.data, hdu.data.dtype)
            hdulist.append(ImageHDU(data=data, header=hdu.header))
        else:
            hdulist.append(hdu.copy())
    return HDUList(hdulist)


def pack(uncompressed_hdulist: HDUList, extension_quantizations: dict | None = None) -> HDUList:
    """
    Pack a FITS HDUList in an equivalent way to fpack from the cfitsio library.

    Parameters
    ----------
    uncompressed_hdulist : `HDUList`
        The uncompressed FITS HDUList to be packed.
    extension_quantizations : dict, optional
        A dictionary specifying the quantization levels for each extension.
        The keys are the extension names (EXTNAME) and the values are the
        quantization levels. If not provided, a default quantization level
        of 64 is used for all extensions.

    Notes
    -----
    If the primary HDU only has header data, then it will remain the primary
    HDU. If the Primary HDU has image data, it will be moved to the first
    extension as is required for a binary table HDU, which is what it is
    stored as internally.
    """
    if extension_quantizations is None:
        extension_quantizations = {}
    if uncompressed_hdulist[0].data is None:
        primary_hdu = PrimaryHDU(header=uncompressed_hdulist[0].header)
        hdulist = [primary_hdu]
    else:
        primary_hdu = PrimaryHDU()
        data = np.ascontiguousarray(uncompressed_hdulist[0].data)
        extname = uncompressed_hdulist[0].header.get("EXTNAME")
        quantize_level = extension_quantizations.get(extname, 64)
        compressed_hdu = CompImageHDU(
            data=data,
            header=uncompressed_hdulist[0].header,
            quantize_level=quantize_level,
            quantize_method=1,
        )
        hdulist = [primary_hdu, compressed_hdu]

    for hdu in uncompressed_hdulist[1:]:
        if isinstance(hdu, ImageHDU):
            if hdu.data is None:
                data = None
            else:
                data = np.ascontiguousarray(hdu.data)
            extname = hdu.header.get("EXTNAME")
            quantize_level = extension_quantizations.get(extname, 64)
            compressed_hdu = CompImageHDU(
                data=data,
                header=hdu.header,
                quantize_level=quantize_level,
                quantize_method=1,
            )
            hdulist.append(compressed_hdu)
        else:
            hdulist.append(hdu)
    return HDUList(hdulist)
