import hashlib
import os
import re
from time import sleep

from ocs_ingester import ingester
from ocs_ingester.exceptions import RetryError, DoNotRetryError, BackoffRetryError, NonFatalDoNotRetryError

from banzai.utils import import_utils
from banzai.logs import get_logger

logger = get_logger()

SMARTSTACK_FILENAME_RE = re.compile(
    r'(?P<prefix>.*-)(?P<frame_number>\d+)-(?P<kind>[ex])(?P<rlevel>\d{2})(?P<suffix>\.fits(?:\.fz)?)$'
)


def get_processed_path(base_path, site, camera, epoch):
    return os.path.join(base_path, site, camera, epoch, 'processed')


def make_smartstack_filename(first_filename, last_filename, reduction_level=45):
    """Build a smartstack FITS filename from the first and last input frame filenames.

    The range comes from the inputs' file frame numbers (unique per camera and
    night), not their stack positions — stack positions always start at 1, so
    two same-night stacks from one camera would collide.

    Parameters
    ----------
    first_filename : str
        Filename of the first reduced input frame.
    last_filename : str
        Filename of the last reduced input frame.
    reduction_level : int, optional
        Output reduction level.

    Returns
    -------
    str
        Smartstack FITS filename with the file frame-number range.

    Raises
    ------
    ValueError
        If either filename cannot be parsed as a reduced FITS filename.
    """
    first_match = SMARTSTACK_FILENAME_RE.match(first_filename)
    if first_match is None:
        raise ValueError(f'Could not parse smartstack input filename: {first_filename}')
    last_match = SMARTSTACK_FILENAME_RE.match(last_filename)
    if last_match is None:
        raise ValueError(f'Could not parse smartstack input filename: {last_filename}')

    frame_number_width = len(first_match.group('frame_number'))
    first_frame = f"{int(first_match.group('frame_number')):0{frame_number_width}d}"
    last_frame = f"{int(last_match.group('frame_number')):0{frame_number_width}d}"
    rlevel = f'{reduction_level:02d}'

    return (
        f"{first_match.group('prefix')}{first_frame}-{last_frame}-"
        f"{first_match.group('kind')}{rlevel}{first_match.group('suffix')}"
    )


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
