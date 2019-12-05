import logging

from banzai.utils.instrument_utils import instrument_passes_criteria


logger = logging.getLogger('banzai')


def image_can_be_processed(image, context):
    # Short circuit if the instrument is a guider even if they don't exist in configdb
    if image.obstype not in context.LAST_STAGE:
        logger.warning('Image has an obstype that is not supported by banzai.', extra_tags={'filename': image.filename})
        return False
    passes = instrument_passes_criteria(image.instrument, context.FRAME_SELECTION_CRITERIA)
    if not passes:
        logger.debug('Image does not pass reduction criteria', extra_tags={'filename': image.filename})
    if not image.is_raw():
        logger.debug('Image is now raw. It has already been processed', extra_tags={'filename': image.filename})
    return passes
