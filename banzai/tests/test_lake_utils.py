import mock
import requests
import json
import pytest
import copy

from banzai.utils import lake_utils
from banzai.tests.utils import FakeInstrument


fake_response_json = {
    'results': [
        {
            "request": {
                "configurations": [
                    {
                        "state": "COMPLETED",
                        'instrument_name': 'fa14',
                        'instrument_type': '1M0-SCICAM-SINISTRO',
                        'type': 'SKY_FLAT',
                        "instrument_configs": [
                            {
                                "exposure_time": 10.0,
                                "exposure_count": 2
                            },
                        ]
                    },
                ]
            },
            'site': 'cpt',
            'enclosure': 'domb',
        },
        {
            'request': {
                "configurations": [
                    {
                        "state": "COMPLETED",
                        'instrument_name': 'kb84',
                        'instrument_type': '0M4-SCICAM-SBIG',
                        'type': 'BIAS',
                        "instrument_configs": [
                            {
                                "exposure_time": 10.0,
                                "exposure_count": 2
                            },
                        ]
                    },
                    {
                        "state": "COMPLETED",
                        'instrument_name': 'kb84',
                        'instrument_type': '0M4-SCICAM-SBIG',
                        'type': 'DARK',
                        "instrument_configs": [
                            {
                                "exposure_time": 10.0,
                                "exposure_count": 2
                            },
                        ]
                    }
                ]
            },
            'site': 'cpt',
            'enclosure': 'domc',
        },
        {
            "request": {
                "configurations": [
                    {
                        "state": "COMPLETED",
                        'instrument_name': 'fa06',
                        'instrument_type': '1M0-SCICAM-SINISTRO',
                        'type': 'BIAS',
                        "instrument_configs": [
                            {
                                "exposure_time": 10.0,
                                "exposure_count": 2
                            },
                        ]
                    },
                    {
                        "state": "COMPLETED",
                        'instrument_name': 'fa06',
                        'instrument_type': '1M0-SCICAM-SINISTRO',
                        'type': 'DARK',
                        "instrument_configs": [
                            {
                                "exposure_time": 10.0,
                                "exposure_count": 2
                            },
                        ]
                    }
                ]
            },
            'site': 'cpt',
            'enclosure': 'domc',
        }
    ]
}

valid_response = [
    {
        "request": {
            "configurations": [
                {
                    'instrument_name': 'fa06',
                    'type': 'BIAS',
                    "instrument_configs": [
                        {
                            "exposure_time": 10.0,
                            "exposure_count": 2
                        },
                    ]
                },
            ]
        },
        'site': 'cpt',
        'enclosure': 'domc',
    }
]


@mock.patch('banzai.utils.lake_utils.requests.get')
def test_can_parse_successful_response(mock_requests):
    mock_response = requests.Response()
    mock_response._content = str.encode(json.dumps(fake_response_json))
    mock_response.status_code = 200
    mock_requests.return_value = mock_response
    blocks = lake_utils.get_calibration_blocks_for_time_range('', '', '')
    fake_response_json['results'][0]['request']['configurations'][0]['type'] = 'SKYFLAT'
    assert blocks == fake_response_json['results']


@mock.patch('banzai.utils.lake_utils.requests.get')
def test_can_parse_unsuccessful_response(mock_requests):
    mock_response = requests.Response()
    mock_response.status_code = 418
    mock_requests.return_value = mock_response
    with pytest.raises(requests.HTTPError):
        lake_utils.get_calibration_blocks_for_time_range('', '', '')


def test_filter_calibration_blocks_for_type():
    fake_inst = FakeInstrument(site='cpt', camera='fa06', enclosure='domc', telescope='2m0a', type='1m0-SciCam-Sinistro')
    filtered_blocks = lake_utils.filter_calibration_blocks_for_type(fake_inst, 'BIAS', fake_response_json['results'])
    expected_response = valid_response
    assert filtered_blocks == expected_response

def test_filter_calibration_blocks_for_type_ignore_empty_observations():
    fake_inst = FakeInstrument(site='cpt', camera='fa06', enclosure='domc', telescope='2m0a', type='1m0-SciCam-Sinistro')
    filtered_blocks = lake_utils.filter_calibration_blocks_for_type(fake_inst, 'SKYFLAT', fake_response_json['results'])
    assert len(filtered_blocks) == 0
