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
            'molecules': [
                {
                    'inst_name': 'fa14',
                    'type': 'SKY_FLAT',
                },
            ],
            'site': 'cpt',
            'observatory': 'domb',
            'instrument_class': '1M0-SCICAM-SINISTRO',
        },
        {
            'molecules': [
                {
                    'inst_name': 'kb84',
                    'type': 'BIAS',
                },
                {
                    'inst_name': 'kb84',
                    'type': 'DARK',
                }
            ],
            'site': 'cpt',
            'observatory': 'domc',
            'instrument_class': '0M4-SCICAM-SBIG',
        },
        {
            'molecules': [
                {
                    'inst_name': 'fa06',
                    'type': 'BIAS',
                },
                {
                    'inst_name': 'fa06',
                    'type': 'DARK',
                }
            ],
            'site': 'cpt',
            'observatory': 'domc',
            'instrument_class': '1M0-SCICAM-SINISTRO',
        }
    ]
}


@mock.patch('banzai.utils.lake_utils.requests.get')
def test_can_parse_successful_response(mock_requests):
    mock_response = requests.Response()
    mock_response._content = str.encode(json.dumps(fake_response_json))
    mock_response.status_code = 200
    mock_requests.return_value = mock_response
    blocks = lake_utils.get_calibration_blocks_for_time_range('', '', '')
    fake_response_json['results'][0]['molecules'][0]['type'] = 'SKYFLAT'
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
    expected_response = [copy.deepcopy(fake_response_json['results'][2])]
    expected_response[0]['molecules'].pop()
    filtered_blocks = lake_utils.filter_calibration_blocks_for_type(fake_inst, 'BIAS', fake_response_json['results'])
    assert filtered_blocks == expected_response
