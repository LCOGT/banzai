__author__ = 'cmccully'

import datetime

def epoch_string_to_date(epoch):
    return datetime.date(int(epoch[0:4]), int(epoch[4:6]), int(epoch[6:8]))

def parse_epoch_string(epoch_string):
    if '-' in epoch_string:
        epoch1, epoch2 = epoch_string.split('-')
        start = epoch_string_to_date(epoch1)
        stop = epoch_string_to_date(epoch2)

        epoch_list = []
        for i in range((stop - start).days + 1):
            epoch = start + datetime.timedelta(days=i)
            epoch_list.append(str(epoch).replace('-',''))
    else:
        epoch_list = [epoch_string]

    return epoch_list