from __future__ import absolute_import, print_function, division
__author__ = 'cmccully'

def sanitizeheader(header):
    # Remove the mandatory keywords from a header so it can be copied to a new
    # image.
    header = header.copy()

    # Let the new data decide what these values should be
    for i in ['SIMPLE', 'BITPIX', 'BSCALE', 'BZERO']:
        if i in header.keys():
            header.pop(i)

    if 'NAXIS' in header.keys():
        naxis = header.pop('NAXIS')
        for i in range(naxis):
            header.pop('NAXIS%i' % (i + 1))

    return header


def split_slice(pixel_section):
    pixels = pixel_section.split(':')
    return slice(int(pixels[0]) - 1, int(pixels[1]))


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