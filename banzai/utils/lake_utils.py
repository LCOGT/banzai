import requests
import logging

logger = logging.getLogger('banzai')

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
    calibration_blocks = []
    for block in blocks:
        if instrument.type.upper() == block['instrument_class'] and instrument.site == block['site'] and instrument.enclosure == block['observatory']:
            for molecule in block['molecules']:
                if calibration_type.upper() == molecule['type'] and instrument.camera == molecule['inst_name']:
                    calibration_blocks.append(block) #TODO: this could append the same block multiple times and should be fixed
                    break
    return calibration_blocks
