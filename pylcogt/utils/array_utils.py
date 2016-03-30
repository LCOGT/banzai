from __future__ import absolute_import, print_function, division


def array_indices_to_slices(a):
    return (slice(0, x, 1) for x in a.shape)