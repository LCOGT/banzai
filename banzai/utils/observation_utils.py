import requests
import logging
import copy

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


def filter_calibration_blocks_for_type(instrument, calibration_type, observations, runtime_context):
    calibration_observations = []
    for observation in observations:
        if instrument.site == observation['site']:
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
