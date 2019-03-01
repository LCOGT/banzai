import requests
import json
import logging

logger = logging.getLogger(__name__)

LAKE_URL = 'http://lake.lco.gtn/blocks/'
CALIBRATE_PROPOSAL_ID = 'calibrate'


def get_next_calibration_blocks(site, start_before, start_after):
    payload = {'start_before': start_before, 'start_after': start_after, 'site': site,
               'proposal': CALIBRATE_PROPOSAL_ID, 'aborted': False, 'canceled': False, 'order': 'start'}
    response = requests.get(LAKE_URL, params=payload)
    response.raise_for_status()

    return response.json()['results']


def get_next_block(instrument, calibration_type, blocks):
    logger.info('get_next_block')
    logger.info(blocks)
    logger.info(instrument.type)
    logger.info(instrument.camera)
    logger.info(calibration_type)
    for block in blocks:
        if instrument.type.upper() == block['instrument_class']:
            for molecule in block['molecules']:
                if calibration_type == molecule['type'] and instrument.camera == molecule['inst_name']:
                    return block


def get_block_by_id(block_id):
    response = requests.get(LAKE_URL + block_id)
    response.raise_for_status()

    return response.json()
