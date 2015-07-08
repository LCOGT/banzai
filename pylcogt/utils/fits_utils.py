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