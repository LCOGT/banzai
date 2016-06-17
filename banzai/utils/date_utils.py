""" date_utils.py: Utility functions for working with dates.

Author
    Curtis McCully (cmccully@lcogt.net)

October 2015
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import datetime
import numpy as np


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
    return str(epoch).replace('-', '')


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


def parse_date_obs(date_obs_string):
    # Check if there is fractional seconds in the time
    date_fractional_seconds_list = date_obs_string.split('.')
    if len(date_fractional_seconds_list) > 1:
        # Pad the string with zeros to include microseconds
        date_obs_string += (6 - len(date_fractional_seconds_list[-1])) * '0'
    else:
        # Pad the string with zeros to include microseconds
        date_obs_string += '.000000'
    return datetime.datetime.strptime(date_obs_string, '%Y-%m-%dT%H:%M:%S.%f')


def date_obs_to_string(date_obs):
    return date_obs.strftime('%Y-%m-%dT%H:%M:%S.%f')


def mean_date(dates):
    time_offsets = np.array([d - min(dates) for d in dates])
    average_offset = total_seconds(time_offsets.sum())/ time_offsets.size
    return min(dates) + datetime.timedelta(seconds=average_offset)


# Necessary for Python 2.6 support. This should go away at some point.
def total_seconds(timedelta):
    seconds = timedelta.seconds + timedelta.days * 24.0 * 3600.0
    microseconds = (timedelta.microseconds + seconds * 1e6)
    return microseconds / 1e6


def get_dayobs(timezone):
    # Get the current utc
    now = datetime.datetime.utcnow()
    # Add the timezone offset
    now += datetime.timedelta(hours=timezone)
    # Assume that the night is over, so we want yesterday's dayobs
    yesterday = now - datetime.timedelta(days=1)
    return epoch_date_to_string(yesterday.date())
