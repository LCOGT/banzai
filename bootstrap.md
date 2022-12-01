# BANZAI Bootstrap Procedure
This is a quick-and-dirty procedure for processing and stacking a set of calibration frames. Run these blocks as-needed inside of a BANZAI pod, using your python interpreter of choice

## Imports
```python
import os
import datetime

import requests

from banzai import settings
import banzai.main
from banzai.celery import process_image, stack_calibrations
from banzai.dbs import get_processed_image, commit_processed_image, mark_frame, get_session, Instrument
```

## Convenience functions
```python
def set_up_context():
    """
    Set up the runtime context the pipeline needs for re-processing
    """
    settings.fpack=True
    settings.db_address = os.getenv('DB_ADDRESS')
    settings.reduction_level = 91

    context = banzai.main.parse_args(settings, parse_system_args=False)
    dict_context = dict(vars(context))
    dict_context['broker_url'] = os.getenv('FITS_BROKER')
    dict_context['queue_name'] = os.getenv('QUEUE_NAME')
    dict_context['post_to_archive'] = True
    dict_context['no_file_cache'] = True

    return dict_context

def fetch_frames_from_archive(base_url, params, collection):
    """
    Call out to archive API and return a response
    """
    response = requests.get(base_url, params=params, headers=settings.ARCHIVE_AUTH_HEADER).json()
    collection += response['results']
    if response.get('next'):
        fetch_frames_from_archive(response['next'], collection)
```

## Process the last N hours of images from the archive
```python
lookback_hours = 4
# Set a min_date and max_date
min_date = (datetime.datetime.utcnow() - datetime.timedelta(hours=lookback_hours)).strftime('%Y-%m-%dT%H:%M:%S')
max_date = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

# Re-process all biases from the archive!
frame_records = []
query_parameters = {'configuration_type': 'BIAS',
                    'instrument_id': 'sq003ms',
                    'start': min_date,
                    'end': max_date}
fetch_frames_from_archive(f'http://archiveapi.archiveapi/frames/', query_parameters, frame_records)

# Mark the corresponding images as un-successful in the ProcessedImages in case they've already been attempted
for record in frame_records:
    processed_image = get_processed_image(record['filename'], db_address=os.getenv('DB_ADDRESS'))
    if processed_image is not None:
        processed_image.success = False
        processed_image.tries = 0
        commit_processed_image(processed_image, db_address=os.getenv('DB_ADDRESS'))

    # Finally, process this image
    record['frameid'] = record['id']
    process_image.apply_async((record, set_up_context()))
```

## Stack the last N hours of images you just processed
Note: Wait until processing has finished!
```python
# Now wait until processing has finished and mark all of these frames as good in the DB!
for record in frame_records:
    mark_frame(record['filename'].replace('EX00', 'EX91'), 'good', os.getenv('DB_ADDRESS'))

# Now stack!
instrument_name = 'sq003ms'
with get_session(os.getenv('DB_ADDRESS')) as session:
    instrument = session.query(Instrument).filter(Instrument.camera==instrument_name).one()

stack_calibrations.apply_async((min_date, max_date, instrument.id, 'BIAS', set_up_context()))
```
