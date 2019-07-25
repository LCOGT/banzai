import mock
import pytest

from datetime import datetime, timedelta

from celery.exceptions import Retry

from banzai.celery import stack_calibrations
from banzai.settings import CALIBRATION_STACK_DELAYS
from banzai.utils import date_utils
from banzai.context import Context
from banzai.tests.utils import FakeInstrument
from banzai.tests.bias_utils import FakeBiasImage

fake_blocks_response_json = {
                        "results": [
                            {
                                "id": 459503917,
                                "molecules": [
                                    {
                                        "id": 974434567,
                                        "prop_id": "calibrate",
                                        "type": "BIAS",
                                        "completed": 'false',
                                        "exposure_count": 2
                                    },
                                    {
                                        "id": 974434568,
                                        "prop_id": "calibrate",
                                        "type": "SKY_FLAT",
                                        "completed": 'false',
                                        "exposure_count": 2
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
                            },
                            {
                                "id": 459503917,
                                "molecules": [
                                    {
                                        "id": 974434567,
                                        "prop_id": "calibrate",
                                        "type": "BIAS",
                                        "completed": 'false',
                                        "exposure_count": 2
                                    },
                                ],
                                "start": "2019-02-20T08:27:49",
                                "end": "2019-02-20T09:55:09",
                                "site": "coj",
                                "observatory": "clma",
                                "telescope": "2m0a",
                                "instrument_class": "2M0-SCICAM-SPECTRAL",
                                "canceled": 'false',
                                "aborted": 'false'
                            }
                        ]
                    }

runtime_context_json = {'site': 'coj', 'min_date': '2019-02-19T20:27:49',
                        'max_date': '2019-02-20T09:55:09', 'frame_type': 'BIAS', 'db_address': 'db_address',
                        'camera': '2m0-SciCam-Spectral', 'enclosure': 'clma', 'telescope': '2m0a'}

fake_instruments_response = FakeInstrument()


class TestMain():
    @mock.patch('banzai.celery.dbs.get_timezone')
    @mock.patch('banzai.celery.stack_calibrations.apply_async')
    @mock.patch('banzai.celery.dbs.get_instruments_at_site')
    @mock.patch('banzai.utils.lake_utils.get_calibration_blocks_for_time_range')
    @mock.patch('banzai.utils.lake_utils.filter_calibration_blocks_for_type')
    def test_submit_stacking_tasks_to_queue_no_delay(self, mock_filter_blocks, mock_get_blocks,     mock_get_instruments, mock_stack_calibrations, mock_get_timezone):
        mock_get_instruments.return_value = [FakeInstrument(site='coj', camera='2m0-SciCam-Spectral',
                                                            enclosure='clma', telescope='2m0a')]
        mock_get_blocks.return_value = fake_blocks_response_json
        mock_filter_blocks.return_value = [block for block in fake_blocks_response_json['results']]
        mock_get_timezone.return_value = 0
        runtime_context = Context(runtime_context_json)
        submit_stacking_tasks_to_queue(runtime_context)
        mock_stack_calibrations.assert_called_with(args=(runtime_context._asdict(), mock_filter_blocks.return_value),
                                               countdown=0)

    @mock.patch('banzai.celery.dbs.get_timezone')
    @mock.patch('banzai.celery.stack_calibrations.apply_async')
    @mock.patch('banzai.celery.dbs.get_instruments_at_site')
    @mock.patch('banzai.utils.lake_utils.get_calibration_blocks_for_time_range')
    @mock.patch('banzai.utils.lake_utils.filter_calibration_blocks_for_type')
    def test_submit_stacking_tasks_to_queue_with_delay(self, mock_filter_blocks, mock_get_blocks,   mock_get_instruments, mock_stack_calibrations, mock_get_timezone):
        mock_get_instruments.return_value = [FakeInstrument(site='coj', camera='2m0-SciCam-Spectral',
                                                            enclosure='clma', telescope='2m0a')]
        fake_blocks_response_json['results'][0]['end'] = datetime.strftime(datetime.utcnow() + timedelta(minutes=1), date_utils.TIMESTAMP_FORMAT)
        mock_get_blocks.return_value = fake_blocks_response_json
        mock_get_timezone.return_value = 0
        mock_filter_blocks.return_value = [block for block in fake_blocks_response_json['results']]
        runtime_context = Context(runtime_context_json)
        submit_stacking_tasks_to_queue(runtime_context)
        mock_stack_calibrations.assert_called_with(args=(runtime_context._asdict(), mock_filter_blocks.return_value),
                                               countdown=(60+CALIBRATION_STACK_DELAYS['BIAS']))

    @mock.patch('banzai.calibrations.process_master_maker')
    @mock.patch('banzai.celery.dbs.get_individual_calibration_images')
    @mock.patch('banzai.celery.dbs.query_for_instrument')
    def test_stack_calibrations(self, mock_query_for_inst, mock_get_calibration_images, mock_process_master_maker):
        fake_inst = FakeInstrument(site='coj', camera='2m0-SciCam-Spectral', enclosure='clma', telescope='2m0a')
        mock_query_for_inst.return_value = fake_inst
        mock_get_calibration_images.return_value = [FakeBiasImage(), FakeBiasImage()]
        stack_calibrations(runtime_context_json, [fake_blocks_response_json['results'][0]])
        mock_process_master_maker.assert_called_with(Context(runtime_context_json), fake_inst, 'BIAS',
                                                     datetime.strptime(runtime_context_json['min_date'], date_utils.TIMESTAMP_FORMAT),
                                                     datetime.strptime(runtime_context_json['max_date'], date_utils.TIMESTAMP_FORMAT))

    @mock.patch('banzai.calibrations.process_master_maker')
    @mock.patch('banzai.celery.dbs.get_individual_calibration_images')
    @mock.patch('banzai.celery.dbs.query_for_instrument')
    def test_stack_calibrations_not_enough_images(self, mock_query_for_inst, mock_get_calibration_images, mock_process_master_maker):
        fake_inst = FakeInstrument(site='coj', camera='2m0-SciCam-Spectral', enclosure='clma', telescope='2m0a')
        mock_query_for_inst.return_value = fake_inst
        mock_get_calibration_images.return_value = [FakeBiasImage()]
        with pytest.raises(Retry) as e:
            stack_calibrations(runtime_context_json, [fake_blocks_response_json['results'][0]], process_any_images=False)
        assert e.type is Retry
