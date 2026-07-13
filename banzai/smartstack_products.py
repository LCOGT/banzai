import datetime
import os

import numpy as np

from banzai.data import CCDData, HeaderOnly, combine_images, science_hdu
from banzai.logs import get_logger
from banzai.utils import date_utils, import_utils
from banzai.utils.file_utils import make_jpg_filenames, make_smartstack_filename
from banzai.utils.jpg_utils import save_jpg, stretch_for_display
from banzai.utils.messaging import post_to_shipper_queue


logger = get_logger()

SMARTSTACK_REDUCTION_LEVEL = 45
STACK_NSIGMA_REJECT = 3.0


def open_stackframe_images(stackframes, runtime_context):
    frame_factory = import_utils.import_attribute(runtime_context.FRAME_FACTORY)()
    images = []
    for stackframe in stackframes:
        image = frame_factory.open({'path': stackframe.filepath}, runtime_context)
        if image is None:
            raise RuntimeError(f'Could not open smartstack input image: {stackframe.filepath}')
        images.append(image)
    return images


def _zeroed_ccd_hdu(ccd_hdu):
    return type(ccd_hdu)(
        data=np.zeros_like(ccd_hdu.data),
        meta=ccd_hdu.meta.copy(),
        name=ccd_hdu.name,
        mask=np.zeros_like(ccd_hdu.mask),
        uncertainty=np.zeros_like(ccd_hdu.uncertainty),
        memmap=ccd_hdu.memmap,
    )


def init_smartstack_frame(first_image, output_filename):
    hdu_list = []
    copied_science_hdu = False
    for hdu in first_image._hdus:
        if isinstance(hdu, HeaderOnly):
            hdu_list.append(HeaderOnly(meta=hdu.meta.copy(), name=hdu.name))
        elif isinstance(hdu, CCDData) and not copied_science_hdu:
            hdu_list.append(_zeroed_ccd_hdu(hdu))
            copied_science_hdu = True

    if not copied_science_hdu:
        raise ValueError('Smartstack inputs must contain at least one CCDData HDU')

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
        exptime = science_hdu(image).meta.get('EXPTIME')
        if exptime is None:
            exptime = image.exptime
        total_exptime += float(exptime or 0.0)
    earliest_dateobs = min(image.dateobs for _, image in sorted_inputs)
    date_created = datetime.datetime.now(datetime.timezone.utc)
    primary_header = output_frame.primary_hdu.meta
    science_header = science_hdu(output_frame).meta
    headers = [primary_header]
    if science_header is not primary_header:
        headers.append(science_header)

    for header in headers:
        header['EXPTIME'] = (total_exptime, '[s] Total exposure time')
        header['DATE-OBS'] = (date_utils.date_obs_to_string(earliest_dateobs), '[UTC] Earliest observation time')
        header['NCOMBINE'] = (len(sorted_inputs), 'Number of images combined')
        header['MOLUID'] = (moluid, 'Observation request UID')
        header['DATE'] = (date_utils.date_obs_to_string(date_created), '[UTC] Date this FITS file was written')
        if 'MOLFRNUM' in header:
            del header['MOLFRNUM']
        header.add_history('Images combined to create smartstack image:')
        for index, (_, image) in enumerate(sorted_inputs, start=1):
            header[f'IMCOM{index:03d}'] = (image.filename, 'Image combined to create smartstack')


def build_stacked_frame(stackframes, runtime_context, moluid):
    if not stackframes:
        raise ValueError('Cannot build a smartstack without stackframes')
    stackframes = sorted(stackframes, key=lambda stackframe: stackframe.stack_num)
    input_images = open_stackframe_images(stackframes, runtime_context)
    output_filename = make_smartstack_filename(
        input_images[0].filename,
        input_images[-1].filename,
        reduction_level=SMARTSTACK_REDUCTION_LEVEL,
    )
    output_frame = init_smartstack_frame(input_images[0], output_filename)
    combine_images(input_images, output_frame, nsigma=STACK_NSIGMA_REJECT, method='sum')
    apply_smartstack_metadata(output_frame, input_images, stackframes, moluid)
    return output_frame


def render_jpgs(output_frame, runtime_context):
    output_directory = output_frame.get_output_directory(runtime_context)
    os.makedirs(output_directory, exist_ok=True)
    small_filename, large_filename = make_jpg_filenames(output_frame.filename)
    small_path = os.path.join(output_directory, small_filename)
    large_path = os.path.join(output_directory, large_filename)
    display_image = stretch_for_display(science_hdu(output_frame).data)
    save_jpg(display_image, large_path, 900)
    save_jpg(display_image, small_path, 300)
    return small_path, large_path


def run_preview(stackframes, runtime_context, moluid):
    output_frame = build_stacked_frame(stackframes, runtime_context, moluid)
    small_path, large_path = render_jpgs(output_frame, runtime_context)
    newest_stackframe = max(stackframes, key=lambda stackframe: stackframe.stack_num)
    post_to_shipper_queue(
        runtime_context.broker_url,
        runtime_context.SHIPPER_EXCHANGE,
        runtime_context.SHIPPER_QUEUE_NAME,
        fits_path=None,
        small_thumbnail=small_path,
        large_thumbnail=large_path,
        instrument_enqueue_timestamp=newest_stackframe.instrument_enqueue_timestamp,
    )
    logger.debug('Published smartstack preview', image=output_frame, extra_tags={'moluid': moluid})


def run_final(stackframes, runtime_context, moluid):
    output_frame = build_stacked_frame(stackframes, runtime_context, moluid)
    output_products = output_frame.write(runtime_context)
    small_path, large_path = render_jpgs(output_frame, runtime_context)
    fits_path = os.path.join(output_products[0].filepath, output_products[0].filename)
    logger.info('Created final smartstack', image=output_frame, extra_tags={'moluid': moluid})
    return fits_path, small_path, large_path
