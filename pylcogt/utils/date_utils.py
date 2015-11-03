""" date_utils.py: Utility functions for working with dates.

Author
    Curtis McCully (cmccully@lcogt.net)

October 2015
"""
from __future__ import absolute_import, print_function, division

import datetime


def epoch_string_to_date(epoch):
    """
    Convert a string in the form of "YYYYMMDD" into a datetime object

    Parameters
    ----------
    epoch : string
        String to convert to a list of datetime objects.
        The string must have the format of YYYYMMDD.

    Returns
    -------
    datetime_epoch : datetime
    """
    return datetime.date(int(epoch[0:4]), int(epoch[4:6]), int(epoch[6:8]))


def epoch_date_to_string(epoch):
    """
    Convert a datetime object to a string with the format YYYYMMDD

    Parameters
    ----------
    epoch : datetime object or string
        datetime object to convert to a string

    Returns
    -------
    epoch_string : string
        Epoch string in the format of YYYYMMDD

    Notes
    -----
    If the input epoch is already string, then it is just returned.
    """
    if isinstance(epoch, basestring):
        epoch_string = epoch
    else:
        epoch_string = str(epoch).replace('-', '')
    return epoch_string


def parse_epoch_string(epoch_string):
    """
    Parse a string into a list of datetime objects

    Parameters
    ----------
    epoch_string : string
        String to convert to a list of datetime objects.
        The string can have the format of YYYYMMDD or YYYYMMDD-YYYYMMDD

    Returns
    -------
    epoch_list : list of datetime objects
    """
    if '-' in epoch_string:
        epoch1, epoch2 = epoch_string.split('-')
        start = epoch_string_to_date(epoch1)
        stop = epoch_string_to_date(epoch2)

        epoch_list = []
        for i in range((stop - start).days + 1):
            epoch = start + datetime.timedelta(days=i)
            epoch_list.append(str(epoch).replace('-', ''))
    else:
        epoch_list = [epoch_string]

    return epoch_list
