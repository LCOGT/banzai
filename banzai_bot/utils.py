import glob
import requests
import datetime
import os
from banzai_bot import settings


def get_missing_frames(camera, dayobs):
    """
    For a given date, get a list of missing frames from the archive
    """
    auth_token = {'Authorization': 'Token ' + settings.ARCHIVE_API_TOKEN}

    dayobs_datetime = datetime.datetime.strptime(dayobs, '%Y%m%d')
    start = (dayobs_datetime - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    end = (dayobs_datetime + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    raw_frames = []
    fetch_frames(archiveurl.format(rlevel=0, instrument=camera, site=site, start=start, end=end, dayobs=dayobs), auth_token, raw_frames)
    processed_frames  = []
    fetch_frames(archiveurl.format(rlevel=91, instrument=camera, site=site, start=start, end=end, dayobs=dayobs), auth_token, processed_frames)
    missing_frames = []
    processed_filenames = [frame['filename'] for frame in processed_frames]
    for raw_frame in raw_frames:
       if raw_frame['filename'].replace('00.fits', '91.fits') not in processed_filenames:
           missing_frames.append(raw_frame)
    print('The following raw frames were not processed:')
    for frame in missing_frames:
       print(frame['filename']) 
    return missing_frames


def fetch_frames(url, authentication_headers, collection):
    response = requests.get(url, headers=authentication_headers).json()
    collection += response['results']
    if response.get('next'):
        fetch_frames(response['next'], authentication_headers, collection)


def get_sites_and_instruments():
    site_info = requests.get(settings.ARCHIVE_API_ROOT + 'frames/aggregate/').json()
    print(site_info['sites'].sort())
    return (site_info['sites'].sort(), site_info['instruments'].sort())
   