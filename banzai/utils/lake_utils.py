import requests
import dramatiq
import time
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
        if instrument.type == block['instrument_class']:
            for molecule in block['molecules']:
                if calibration_type == molecule['type'] and instrument.camera == molecule['inst_name']:
                    return block


@dramatiq.actor(max_retries=3, min_backoff=1000*60*10)
def schedule_stack(block_id, calibration_type, instrument):
    response = requests.get(LAKE_URL + block_id)
    reponse.raise_for_status()
    result = response.json()
    for molecule in molecules:
        if not molecule['completed']:
            raise Exception


def schedule_stacking_checks(site, pipeline_context):
    now = datetime.utcnow()
    calibration_blocks = get_next_calibration_blocks(site, now, now+timedelta(days=1))
    instruments = dbs.get_instruments_at_site(site=site, db_address=pipeline_context.db_address)
    for instrument in instruments:
        for calibration_type in pipeline_context.CALIBRATION_IMAGE_TYPES:
            block_for_calibration = get_next_block(instrument, calibration_type, calibration_blocks)
            if block_for_calibration is not None:
                block_end = datetime.strptime(block_for_calibration['end'], '%Y-%m-%dT%H:%M:%S')
                stack_delay = timedelta(milliseconds=pipeline_context.CALIBRATION_STACK_DELAYS['calibration_type'])
                message_delay = now - block_end + stack_delay
                schedule_stack.send_with_options(args=(block_for_calibration['id'],
                    calibration_type, instrument), delay=message_delay)
