from __future__ import absolute_import, division, print_function, unicode_literals


def array_indices_to_slices(a):
    return tuple(slice(0, x, 1) for x in a.shape)