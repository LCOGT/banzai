import requests
import logging

logger = logging.getLogger(__name__)

LAKE_URL = 'http://lake.lco.gtn/blocks/'
CALIBRATE_PROPOSAL_ID = 'calibrate'


def get_calibration_blocks_for_time_range(site, start_before, start_after):
    payload = {'start_before': start_before, 'start_after': start_after, 'site': site,
               'proposal': CALIBRATE_PROPOSAL_ID, 'aborted': False, 'canceled': False, 'order': '-start'}
    response = requests.get(LAKE_URL, params=payload)
    response.raise_for_status()
    results = response.json()['results']
    for block in results:
        for molecule in block['molecules']:
            molecule['type'] = molecule['type'].replace('_', '')
    return results


def filter_calibration_blocks_for_type(instrument, calibration_type, blocks):
    logger.info(instrument, calibration_type)
    calibration_blocks = []
    for block in blocks:
        logger.info(block)
        if instrument.type.upper() == block['instrument_class'] and instrument.site == block['site'] and instrument.enclosure == block['observatory']:
            logger.info(instrument.type)
            for molecule in block['molecules']:
                logger.info(molecule)
                logger.info(instrument.camera)
                if calibration_type.upper() == molecule['type'] and instrument.camera == molecule['inst_name']:
                    calibration_blocks.append(block) #this could append the same block multiple times and should be fixed
    return calibration_blocks


def get_block_by_id(block_id):
    response = requests.get(LAKE_URL + str(block_id))
    response.raise_for_status()
    block = response.json()
    for molecule in block['molecules']:
        molecule['type'] = molecule['type'].replace('_', '')

    return block
