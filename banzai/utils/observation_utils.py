import requests
import logging
import copy
from datetime import datetime
from dateutil.parser import parse

logger = logging.getLogger('banzai')


def get_calibration_blocks_for_time_range(site, start_before, start_after, context):
    payload = {'start_before': start_before, 'start_after': start_after, 'site': site,
               'proposal': context.CALIBRATE_PROPOSAL_ID, 'aborted': 'false', 'canceled': 'false', 'order': '-start',
               'offset': '', 'limit': 1000}
    response = requests.get(context.OBSERVATION_PORTAL_URL, params=payload)
    response.raise_for_status()
    results = response.json()['results']
    for observation in results:
        for config in observation['request']['configurations']:
            config['type'] = config['type'].replace('_', '')
    return results


def filter_calibration_blocks_for_type(instrument, calibration_type, observations, runtime_context,
                                       min_date: str, max_date: str):
    min_date = parse(min_date).replace(tzinfo=None)
    max_date = parse(max_date).replace(tzinfo=None)
    calibration_observations = []
    for observation in observations:
        observation_start = parse(observation['start']).replace(tzinfo=None)
        observation_end = parse(observation['end']).replace(tzinfo=None)
        if instrument.site == observation['site'] and observation_start > min_date and observation_end < max_date:
            filtered_observation = copy.deepcopy(observation)
            filtered_observation['request']['configurations'] = []
            for configuration in observation['request']['configurations']:
                request_type = runtime_context.OBSERVATION_REQUEST_TYPES.get(calibration_type.upper(),
                                                                             calibration_type.upper())
                # Move on if anything about the request doesn't match
                if request_type != configuration['type']:
                    continue
                if instrument.name != configuration['instrument_name']:
                    continue
                filtered_observation['request']['configurations'].append(configuration)
            if len(filtered_observation['request']['configurations']) != 0:
                calibration_observations.append(filtered_observation)
    return calibration_observations
