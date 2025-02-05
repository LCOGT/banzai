import mock

from datetime import datetime, timedelta
import pytest

from banzai.utils import date_utils

pytestmark = pytest.mark.date_utils

test_site_data = {
    'coj': {
        'timezone': 10,
        'schedule_time': timedelta(hours=6, minutes=30),
        'dayobs': '20190501',
        'expected_min_date': datetime(2019, 4, 30, 14),
        'expected_max_date': datetime(2019, 5, 1, 14)
    },
    'cpt': {
        'timezone': 2,
        'schedule_time': timedelta(hours=15, minutes=0),
        'dayobs': '20190501',
        'expected_min_date': datetime(2019, 4, 30, 22),
        'expected_max_date': datetime(2019, 5, 1, 22)
    },
    'tfn': {
        'timezone': 1,
        'schedule_time': timedelta(hours=17, minutes=30),
        'expected_min_date': datetime(2019, 4, 30, 23),
        'dayobs': '20190501',
        'expected_max_date': datetime(2019, 5, 1, 23)
    },
    'lsc': {
        'timezone': -4,
        'schedule_time': timedelta(hours=21, minutes=0),
        'dayobs': '20190501',
        'expected_min_date': datetime(2019, 5, 1, 4),
        'expected_max_date': datetime(2019, 5, 2, 4)
    },
    'elp': {
        'timezone': -6,
        'schedule_time': timedelta(hours=23, minutes=0),
        'dayobs': '20190501',
        'expected_min_date': datetime(2019, 5, 1, 6),
        'expected_max_date': datetime(2019, 5, 2, 6)
    },
    'ogg': {
        'timezone': -10,
        'schedule_time': timedelta(hours=3, minutes=0),
        'dayobs': '20190430',
        'expected_min_date': datetime(2019, 4, 30, 10),
        'expected_max_date': datetime(2019, 5, 1, 10)
    }
}


@mock.patch('banzai.utils.date_utils.datetime.datetime')
def test_get_dayobs(mock_datetime):
    for data in test_site_data.values():
        mock_datetime.now = mock.Mock(return_value=datetime(2019, 5, 1) + data['schedule_time'])
        assert date_utils.get_dayobs(data['timezone']) == data['dayobs']


@mock.patch('banzai.utils.date_utils.get_dayobs')
def test_get_expected_min_and_max_dates_for_calibration_scheduling(mock_get_dayobs):
    for data in test_site_data.values():
        mock_get_dayobs.return_value = data['dayobs']
        calculated_min_date, calculated_max_date = date_utils.get_stacking_date_range(data['timezone'])
        assert calculated_min_date == data['expected_min_date'].strftime(date_utils.TIMESTAMP_FORMAT)
        assert calculated_max_date == data['expected_max_date'].strftime(date_utils.TIMESTAMP_FORMAT)
