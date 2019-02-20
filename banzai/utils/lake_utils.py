import requests
from datetime import datetime, timedelta
from banzai import dbs

LAKE_URL = 'http://lake.lco.gtn/blocks/'
CALIBRATE_PROPOSAL_ID = 'calibrate'


def get_next_calibration_blocks(site, start_before, start_after):
    payload = {'start_before': start_before, 'start_after': start_after, 'site': site,
               'proposal': CALIBRATE_PROPOSAL_ID, 'aborted': False, 'canceled': False, 'order': 'start'}
    response = requests.get(LAKE_URL, params=payload)
    response.raise_for_status()

    return response.json()['results']


def get_next_block(instrument, calibration_type, blocks):
    for block in blocks:
        for molecule in block['molecules']:
            if calibration_type == molecule['type'] and molecule['inst_name'] == instrument.camera:
                return block


def schedule_stacking_checks(site, pipeline_context):
    now = datetime.utcnow()
    calibration_blocks = get_next_calibration_blocks(site, now, now+timedelta(days=1))
    instruments = dbs.get_instruments_at_site(site=site, db_address=pipeline_context.db_address)
    for instrument in instruments:
        for calibration_type in pipeline_context.CALIBRATION_IMAGE_TYPES:
            block_for_calibration = get_next_block(instrument, calibration_type, calibration_blocks)
            if block is not None:
                schedule_stack(block_for_calibration['end'] + pipeline_context.STACK_DELAYS[calibration_type],
                               block_for_calibration['id'], calibration_type, instrument)


