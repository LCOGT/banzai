import pytest
import mock
import requests
import json
from importlib_resources import read_text

import banzai_bot.utils as utils

def load_json(filename):
    return json.loads(read_text('banzai_bot.test.fixtures', filename))
