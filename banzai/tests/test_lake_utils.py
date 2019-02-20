from banzai.utils import lake_utils
import mock
import requests
import json
import pytest


fake_response_json = {
                        "results": [
                            {
                                "id": 459503917,
                                "molecules": [
                                    {
                                        "id": 974434567,
                                        "prop_id": "calibrate",
                                        "type": "BIAS",
                                        "completed": 'false',
                                    },
                                ],
                                "start": "2019-02-19T20:27:49",
                                "end": "2019-02-19T21:55:09",
                                "site": "coj",
                                "observatory": "clma",
                                "telescope": "2m0a",
                                "instrument_class": "2M0-SCICAM-SPECTRAL",
                                "canceled": 'false',
                                "aborted": 'false'
                            }
                        ]
                    }


@mock.patch('banzai.utils.lake_utils.requests.get')
def test_can_parse_successful_response(mock_requests):
    mock_response = requests.Response()
    mock_response._content = str.encode(json.dumps(fake_response_json))
    mock_response.status_code = 200
    mock_requests.return_value = mock_response
    blocks = lake_utils.get_next_calibration_blocks('', '', '')
    assert blocks == fake_response_json['results']


@mock.patch('banzai.utils.lake_utils.requests.get')
def test_can_parse_unsuccessful_response(mock_requests):
    mock_response = requests.Response()
    mock_response.status_code = 418
    mock_requests.return_value = mock_response
    with pytest.raises(requests.HTTPError):
        lake_utils.get_next_calibration_blocks('', '', '')
