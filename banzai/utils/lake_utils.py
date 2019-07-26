import requests
import logging
import copy
from banzai.settings import CALIBRATE_PROPOSAL_ID, LAKE_URL

logger = logging.getLogger('banzai')


def get_calibration_blocks_for_time_range(site, start_before, start_after):
    payload = {'start_before': start_before, 'start_after': start_after, 'site': site,
               'proposal': CALIBRATE_PROPOSAL_ID, 'aborted': 'false', 'canceled': 'false', 'order': '-start',
               'offset': ''}
    response = requests.get(LAKE_URL, params=payload)
    response.raise_for_status()
    results = response.json()['results']
    for block in results:
        for molecule in block['molecules']:
            molecule['type'] = molecule['type'].replace('_', '')
    return results


def filter_calibration_blocks_for_type(instrument, calibration_type, blocks):
    calibration_blocks = []
    for block in blocks:
        if instrument.type.upper() == block['instrument_class'] and instrument.site == block['site'] and instrument.enclosure == block['observatory']:
            filtered_block = copy.deepcopy(block)
            filtered_block['molecules'] = []
            for molecule in block['molecules']:
                if calibration_type.upper() == molecule['type'] and instrument.name == molecule['inst_name']:
                    filtered_block['molecules'].append(molecule)
            calibration_blocks.append(filtered_block)
    return calibration_blocks
