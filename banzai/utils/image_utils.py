import logging

from banzai import dbs
from banzai.utils.instrument_utils import instrument_passes_criteria
from banzai.exceptions import InhomogeneousSetException


logger = logging.getLogger('banzai')


def check_image_homogeneity(images, group_by_attributes=None):
    attribute_list = ['nx', 'ny', 'site', 'camera']
    if group_by_attributes is not None:
        attribute_list += group_by_attributes
    for attribute in attribute_list:
        if len(set([getattr(image, attribute) for image in images])) > 1:
            raise InhomogeneousSetException('Images have different {0}s'.format(attribute))


def image_can_be_processed(header, context, filename):
    if header is None:
        logger.warning('Header being checked to process image is None', extra_tags={'filename': filename})
        return False
    # Short circuit if the instrument is a guider even if they don't exist in configdb
    if not header.get('OBSTYPE') in context.LAST_STAGE:
        logger.warning('Image has an obstype that is not supported by banzai.', extra_tags={'filename': filename})
        return False
    try:
        instrument = dbs.get_instrument(header, db_address=context.db_address)
    except ValueError:
        return False
    passes = instrument_passes_criteria(instrument, context.FRAME_SELECTION_CRITERIA)
    if not passes:
        logger.debug('Image does not pass reduction criteria', extra_tags={'filename': filename})
    reduction_level = header.get('RLEVEL', '00')
    passes &= reduction_level == '00'
    if reduction_level != '00':
        logger.debug('Image has nonzero reduction level', extra_tags={'filename': filename})
    return passes
