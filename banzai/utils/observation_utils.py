import requests
import logging
import copy
from banzai.settings import CALIBRATE_PROPOSAL_ID, OBSERVATION_PORTAL_URL

logger = logging.getLogger('banzai')


def get_calibration_blocks_for_time_range(site, start_before, start_after):
    payload = {'start_before': start_before, 'start_after': start_after, 'site': site,
               'proposal': CALIBRATE_PROPOSAL_ID, 'aborted': 'false', 'canceled': 'false', 'order': '-start',
               'offset': ''}
    response = requests.get(OBSERVATION_PORTAL_URL, params=payload)
    response.raise_for_status()
    results = response.json()['results']
    for observation in results:
        for config in observation['request']['configurations']:
            config['type'] = config['type'].replace('_', '')
    return results


def filter_calibration_blocks_for_type(instrument, calibration_type, observations):
    calibration_observations = []
    for observation in observations:
        if instrument.site == observation['site'] and instrument.enclosure == observation['enclosure']:
            filtered_observation = copy.deepcopy(observation)
            filtered_observation['request']['configurations'] = []
            for configuration in observation['request']['configurations']:
                if calibration_type.upper() == configuration['type'] and instrument.type.upper() == configuration['instrument_type'] and instrument.camera == configuration['instrument_name']:
                    filtered_observation['request']['configurations'].append(configuration)
            if len(filtered_observation['request']['configurations']) != 0:
                calibration_observations.append(filtered_observation)
    return calibration_observations
