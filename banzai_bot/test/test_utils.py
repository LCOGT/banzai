import pytest
import mock
import requests
import json
from importlib_resources import read_text

import banzai_bot.utils as utils

def load_json(filename):
    return json.loads(read_text('banzai_bot.test.fixtures', filename))


@mock.patch('requests.get')
def test_get_sites_and_instruments(mock_request):
    mock_request.return_value = requests.Request('GET', json=load_json('sites_and_instruments.json'))
    sites, instruments = utils.get_sites_and_instruments()

    site_info = load_json('sites_and_instruments.json')
    site_info['sites'].sort()
    site_info['instruments'].sort() 

    assert sites == site_info['sites']
    assert instruments == site_info['instruments']

