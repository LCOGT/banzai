import logging

import numpy as np

from banzai.utils.instrument_utils import instrument_passes_criteria


logger = logging.getLogger('banzai')


def get_reduction_level(header):
    reduction_level =  header.get('RLEVEL', '00')
    # return a correctly-formatted RLEVEL string - 00 or 91
    return '{:02d}'.format(int(reduction_level))


def image_can_be_processed(image, context):
    # Short circuit if the instrument is a guider even if they don't exist in configdb
    if image.obstype not in context.SUPPORTED_FRAME_TYPES:
        logger.debug('Image has an obstype that is not supported.', extra_tags={'filename': image.filename})
        return False
    passes = instrument_passes_criteria(image.instrument, context.FRAME_SELECTION_CRITERIA)
    if not passes:
        logger.debug('Image does not pass reduction criteria', extra_tags={'filename': image.filename})
    return passes


class Section:
    def __init__(self, x_start, x_stop, y_start, y_stop):
        """
        All 1 indexed inclusive (ala IRAF)
        :param x_start:
        :param x_stop:
        :param y_start:
        :param y_stop:
        """
        self.x_start = x_start
        self.x_stop = x_stop
        self.y_start = y_start
        self.y_stop = y_stop

    def to_slice(self):
        """
        Return a numpy-compatible pixel section
        """
        if None in [self.x_start, self.x_stop, self.y_start, self.y_stop]:
            return None

        y_slice = self._section_to_slice(self.y_start, self.y_stop)
        x_slice = self._section_to_slice(self.x_start, self.x_stop)

        return y_slice, x_slice

    def _section_to_slice(self, start, stop):
        """
        Given a start and stop pixel in IRAF coordinates, convert to a
        numpy-compatible slice.
        """
        if stop > start:
            pixel_slice = slice(start - 1, stop, 1)
        else:
            if stop == 1:
                pixel_slice = slice(start - 1, None, -1)
            else:
                pixel_slice = slice(start - 1, stop - 2, -1)

        return pixel_slice

    @property
    def shape(self):
        return np.abs(self.y_stop - self.y_start) + 1, np.abs(self.x_stop - self.x_start) + 1

    def overlap(self, section):
        return Section(max(min(section.x_start, section.x_stop), min(self.x_start, self.x_stop)),
                       min(max(section.x_start, section.x_stop), max(self.x_start, self.x_stop)),
                       max(min(section.y_start, section.y_stop), min(self.y_start, self.y_stop)),
                       min(max(section.y_start, section.y_stop), max(self.y_start, self.y_stop)))

    @classmethod
    def parse_region_keyword(cls, keyword_value):
        """
        Convert a header keyword of the form [x1:x2],[y1:y2] into a Section object
        :param keyword_value: Header keyword string
        :return: x, y index slices
        """
        if not keyword_value:
            return None
        elif keyword_value.lower() == 'unknown':
            return None
        elif keyword_value.lower() == 'n/a':
            return None
        else:
            # Strip off the brackets and split the coordinates
            pixel_sections = keyword_value[1:-1].split(',')
            x_start, x_stop = pixel_sections[0].split(':')
            y_start, y_stop = pixel_sections[1].split(':')
        return cls(int(x_start), int(x_stop), int(y_start), int(y_stop))

    def to_region_keyword(self):
        return f'[{self.x_start}:{self.x_stop},{self.y_start}:{self.y_stop}]'
