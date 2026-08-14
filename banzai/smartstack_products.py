import datetime
import os

import numpy as np

from banzai.context import Context
from banzai.data import CCDData, HeaderOnly, combine_images, create_combination_output_hdu
from banzai.logs import get_logger
from banzai.utils import date_utils, import_utils
from banzai.utils.background_utils import background_header_cards, estimate_background
from banzai.utils.file_utils import make_jpg_filenames
from banzai.utils.jpg_utils import save_jpg, stretch_for_display
from banzai.utils.messaging import post_to_shipper_queue


logger = get_logger()

STACK_NSIGMA_REJECT = 3.0
SMARTSTACK_REDUCTION_LEVEL = 45

THUMBNAIL_HEADER_KEYS = (
    'DATE-OBS',
    'DAY-OBS',
    'INSTRUME',
    'SITEID',
    'TELID',
    'MOLUID',
    'NCOMBINE',
    'FRMTOTAL',
    'PROPID',
    'OBSTYPE',
    'BLKUID',
    'REQNUM',
    'TRACKNUM',
    'EXPTIME',
    'OBJECT',
    'FILTER',
    'L1PUBDAT',
)
REQUIRED_THUMBNAIL_METADATA = (
    'frame_basename',
    'DATE-OBS',
    'DAY-OBS',
    'INSTRUME',
    'SITEID',
    'TELID',
    'MOLUID',
    'NCOMBINE',
    'FRMTOTAL',
)


def open_stackframe_images(stackframes, runtime_context):
    frame_factory = import_utils.import_attribute(runtime_context.FRAME_FACTORY)()
    images = []
    for stackframe in stackframes:
        image = frame_factory.open({'path': stackframe.filepath}, runtime_context)
        if image is None:
            raise RuntimeError(f'Could not open smartstack input image: {stackframe.filepath}')
        images.append(image)
    return images


def init_smartstack_frame(first_image, output_filename):
    science_hdu = first_image['SCI']
    if not isinstance(science_hdu, CCDData):
        raise ValueError('Smartstack inputs must contain a SCI CCDData HDU')

    hdu_list = []
    for hdu in first_image._hdus:
        if isinstance(hdu, HeaderOnly):
            hdu_list.append(HeaderOnly(meta=hdu.meta.copy(), name=hdu.name))
        elif hdu is science_hdu:
            hdu_list.append(create_combination_output_hdu(science_hdu, science_hdu.meta,
                                                          memmap=science_hdu.memmap))

    hdu_order = first_image.hdu_order
    if hdu_list and isinstance(hdu_list[0], HeaderOnly):
        primary_name = hdu_list[0].name
        if primary_name not in (None, ''):
            raise ValueError('Smartstack primary HeaderOnly HDU must have a blank name')
        hdu_order = [''] + [hdu_name for hdu_name in (first_image.hdu_order or []) if hdu_name != '']

    output_frame = first_image.__class__(hdu_list, output_filename, frame_id=None, hdu_order=hdu_order)
    output_frame.instrument = first_image.instrument
    return output_frame


def apply_smartstack_metadata(output_frame, input_images, stackframes, moluid):
    sorted_inputs = sorted(zip(stackframes, input_images), key=lambda item: item[0].stack_num)

    total_exptime = 0.0
    for _, image in sorted_inputs:
        exptime = image['SCI'].meta.get('EXPTIME')
        if exptime is None:
            exptime = image.exptime
        total_exptime += float(exptime or 0.0)
    earliest_dateobs = min(image.dateobs for _, image in sorted_inputs)
    date_created = datetime.datetime.now(datetime.timezone.utc)
    primary_header = output_frame.primary_hdu.meta
    science_header = output_frame['SCI'].meta
    headers = [primary_header]
    if science_header is not primary_header:
        headers.append(science_header)

    # The stack is a sum renormalized per-pixel by N/n_good (see data.stack), so every pixel is on an
    # N-frame-equivalent flux scale: N * SATURATE/MAXLIN are exact upper bounds for trustworthy values,
    # and summed read-noise variances give sqrt(N) * RDNOISE.
    n_stacked = len(sorted_inputs)
    keyword_scale_factors = {'SATURATE': n_stacked, 'MAXLIN': n_stacked, 'RDNOISE': np.sqrt(n_stacked)}
    # UTSTOP from the newest input so DATE-OBS (earliest) and UTSTOP bracket the full time span.
    newest_image = sorted_inputs[-1][1]
    utstop = newest_image['SCI'].meta.get('UTSTOP')
    if utstop is None:
        utstop = newest_image.primary_hdu.meta.get('UTSTOP')

    # The inherited L1MEAN/L1MEDIAN/L1SIGMA describe the first input, not the stack. Remeasure with the
    # same background-map method as photometry.SourceDetector so e45 values are comparable to e09 ones
    # (raw-pixel stats would inflate L1SIGMA with pixel noise; the map measures background variation).
    background_cards = background_header_cards(estimate_background(output_frame['SCI'].data))

    for header in headers:
        header['EXPTIME'] = (total_exptime, '[s] Total exposure time')
        header['DATE-OBS'] = (date_utils.date_obs_to_string(earliest_dateobs), '[UTC] Earliest observation time')
        header['NCOMBINE'] = (n_stacked, 'Number of images combined')
        header['MOLUID'] = (moluid, 'Observation request UID')
        header['DATE'] = (date_utils.date_obs_to_string(date_created), '[UTC] Date this FITS file was written')
        # MOLFRNUM is a per-exposure sequence number; the combined product has no single position.
        if 'MOLFRNUM' in header:
            del header['MOLFRNUM']
        for keyword, scale_factor in keyword_scale_factors.items():
            inherited_value = header.get(keyword)
            if inherited_value is not None:
                header[keyword] = inherited_value * scale_factor
        if utstop is not None:
            header['UTSTOP'] = utstop
        for keyword, card in background_cards.items():
            header[keyword] = card
        header.add_history('Images combined to create smartstack image:')
        for index, (_, image) in enumerate(sorted_inputs, start=1):
            header[f'IMCOM{index:03d}'] = (image.filename, 'Image combined to create smartstack')


def build_stacked_frame(stackframes, runtime_context, moluid):
    if not stackframes:
        raise ValueError('Cannot build a smartstack without stackframes')
    stackframes = sorted(stackframes, key=lambda stackframe: stackframe.stack_num)
    input_images = open_stackframe_images(stackframes, runtime_context)
    base, rlevel, file_extension = input_images[0].filename.rpartition('-e09')
    if rlevel != '-e09' or file_extension not in ('.fits', '.fits.fz'):
        raise ValueError(f'Could not parse smartstack input filename: {input_images[0].filename}')
    output_filename = f'{base}-e{SMARTSTACK_REDUCTION_LEVEL}{file_extension}'
    output_frame = init_smartstack_frame(input_images[0], output_filename)
    combine_images([image['SCI'] for image in input_images], output_frame['SCI'],
                   nsigma=STACK_NSIGMA_REJECT, method='sum')
    apply_smartstack_metadata(output_frame, input_images, stackframes, moluid)
    return output_frame


def build_thumbnail_metadata(output_frame):
    """Build the aggregate metadata submitted with a JPEG-only preview."""
    frame_basename = output_frame.filename
    for extension in ('.fits.fz', '.fits'):
        if frame_basename.endswith(extension):
            frame_basename = frame_basename[:-len(extension)]
            break
    else:
        raise ValueError(f'Could not parse smartstack output filename: {output_frame.filename}')

    header = output_frame.primary_hdu.meta
    metadata = {'frame_basename': frame_basename, 'RLEVEL': SMARTSTACK_REDUCTION_LEVEL}
    for key in THUMBNAIL_HEADER_KEYS:
        value = header.get(key)
        if value is not None and not (isinstance(value, str) and not value.strip()):
            metadata[key] = value

    missing_keys = [
        key for key in REQUIRED_THUMBNAIL_METADATA
        if key not in metadata or metadata[key] is None
        or (isinstance(metadata[key], str) and not metadata[key].strip())
    ]
    if missing_keys:
        raise ValueError(f'Missing required smartstack thumbnail metadata: {", ".join(missing_keys)}')
    return metadata


def render_jpgs(output_frame, runtime_context):
    output_directory = output_frame.get_output_directory(runtime_context)
    os.makedirs(output_directory, exist_ok=True)
    small_filename, large_filename = make_jpg_filenames(output_frame.filename)
    small_path = os.path.join(output_directory, small_filename)
    large_path = os.path.join(output_directory, large_filename)
    display_image = stretch_for_display(output_frame['SCI'].data)
    save_jpg(display_image, large_path, 900)
    save_jpg(display_image, small_path, 300)
    return small_path, large_path


def run_preview(stackframes, runtime_context, moluid):
    output_frame = build_stacked_frame(stackframes, runtime_context, moluid)
    thumbnail_metadata = build_thumbnail_metadata(output_frame)
    small_path, large_path = render_jpgs(output_frame, runtime_context)
    post_to_shipper_queue(
        runtime_context.broker_url,
        runtime_context.SHIPPER_EXCHANGE,
        runtime_context.SHIPPER_QUEUE_NAME,
        fits_path=None,
        small_thumbnail=small_path,
        large_thumbnail=large_path,
        thumbnail_metadata=thumbnail_metadata,
    )
    logger.debug('Published smartstack preview', image=output_frame, extra_tags={'moluid': moluid})


def run_final(stackframes, runtime_context, moluid):
    output_frame = build_stacked_frame(stackframes, runtime_context, moluid)
    write_context = Context({**vars(runtime_context), 'reduction_level': SMARTSTACK_REDUCTION_LEVEL})
    output_products = output_frame.write(write_context)
    small_path, large_path = render_jpgs(output_frame, runtime_context)
    fits_path = os.path.join(output_products[0].filepath, output_products[0].filename)
    logger.info('Created final smartstack', image=output_frame, extra_tags={'moluid': moluid})
    return fits_path, small_path, large_path
