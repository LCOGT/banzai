from __future__ import absolute_import, division, print_function, unicode_literals
import numpy as np


def array_indices_to_slices(a):
    return tuple(slice(0, x, 1) for x in a.shape)


def prune_nans_from_table(table):
    nan_in_row = np.zeros(len(table), dtype=bool)
    for col in table.colnames:
        nan_in_row |= np.isnan(table[col])
    return table[~nan_in_row]


def sort_slice(section):
    if section.start > section.stop:
        sorted_slice = slice(section.stop + 1, section.start + 1)
    else:
        sorted_slice = slice(section.start, section.stop)
    return sorted_slice


def slice_overlap(slice1, slice2):
    sorted_slice1 = sort_slice(slice1)
    sorted_slice2 = sort_slice(slice2)
    return slice(max(sorted_slice1.start, sorted_slice2.start),
                 min(sorted_slice1.stop, sorted_slice2.stop))
