import mock
import pytest
import dramatiq

from datetime import datetime, timedelta

from dramatiq.brokers.stub import StubBroker
from dramatiq.actor import Actor

from banzai.main import schedule_stacking_checks
from banzai.settings import CALIBRATION_STACK_DELAYS
from banzai.utils import date_utils
from banzai.context import Context
from banzai.tests.utils import FakeInstrument

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
                                    },
                                    {
                                        "id": 974434568,
                                        "prop_id": "calibrate",
                                        "type": "SKY_FLAT",
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

fake_instruments_response = FakeInstrument()


@dramatiq.actor()
def foo(*args, **kwargs):
    return


@pytest.fixture
def stub_broker():
    broker = StubBroker()
    dramatiq.set_broker(broker)
    yield broker
    broker.flush_all()
    broker.close()


@pytest.fixture
def stub_worker(stub_broker):
    worker = dramatiq.Worker(stub_broker)
    worker.start()
    yield worker
    worker.stop()


broker = StubBroker()
worker = stub_worker(broker)
actor = Actor(fn=foo, broker=broker, actor_name='', queue_name='schedule_stack', priority=1, options={})


class TestMain():
    @classmethod
    def setup(cls):
        broker.flush_all()


    @mock.patch('banzai.main.schedule_stack.send_with_options', side_effect=actor.send_with_options)
    @mock.patch('banzai.main.dbs.get_instruments_at_site')
    @mock.patch('banzai.main.lake_utils.get_calibration_blocks_for_time_range')
    @mock.patch('banzai.main.lake_utils.filter_calibration_blocks_for_type')
    def test_schedule_stacking_checks_queues_task_no_delay(self, mock_filter_blocks, mock_get_blocks,     mock_get_instruments, mock_schedule_stack):
        mock_get_instruments.return_value = [FakeInstrument(site='coj', camera='2m0-SciCam-Spectral',
                                                            enclosure='clma', telescope='2m0a')]
        mock_get_blocks.return_value = fake_blocks_response_json
        mock_filter_blocks.return_value = [fake_blocks_response_json['results'][0]]
        runtime_context = Context({'site': 'coj', 'min_date': '2019-02-19T20:27:49',
                                   'max_date': '2019-02-19T21:55:09', 'db_address': 'db_address',
                                   'frame_type': 'BIAS'})
        schedule_stacking_checks(runtime_context)
        mock_schedule_stack.assert_called_with(args=(runtime_context._asdict(), mock_filter_blocks.return_value,    'BIAS', 'coj', '2m0-SciCam-Spectral', 'clma', '2m0a'), delay=0, kwargs={'process_any_images', False},  on_failure=mock.ANY)
        assert broker.queues['schedule_stack.DQ'].qsize() == 1

    @mock.patch('banzai.main.schedule_stack.send_with_options', side_effect=actor.send_with_options)
    @mock.patch('banzai.main.dbs.get_instruments_at_site')
    @mock.patch('banzai.main.lake_utils.get_calibration_blocks_for_time_range')
    @mock.patch('banzai.main.lake_utils.filter_calibration_blocks_for_type')
    def test_schedule_stacking_checks_queues_task_with_delay(self, mock_filter_blocks, mock_get_blocks,   mock_get_instruments, mock_schedule_stack):
        mock_get_instruments.return_value = [FakeInstrument(site='coj', camera='2m0-SciCam-Spectral',
                                                            enclosure='clma', telescope='2m0a')]
        fake_blocks_response_json['results'][0]['end'] = datetime.strftime(datetime.utcnow() + timedelta(minutes=1),     date_utils.TIMESTAMP_FORMAT)
        mock_get_blocks.return_value = fake_blocks_response_json
        mock_filter_blocks.return_value = [fake_blocks_response_json['results'][0]]
        runtime_context = Context({'site': 'coj', 'min_date': '2019-02-19T20:27:49',
                                   'max_date': '2019-02-19T21:55:09', 'db_address': 'db_address',
                                   'frame_type': 'BIAS'})
        schedule_stacking_checks(runtime_context)
        mock_schedule_stack.assert_called_with(args=(runtime_context._asdict(), mock_filter_blocks.return_value,
                                                     'BIAS', 'coj', '2m0-SciCam-Spectral', 'clma', '2m0a'),
                                               delay=(60000+CALIBRATION_STACK_DELAYS['BIAS']),
                                               kwargs={'process_any_images', False}, on_failure=mock.ANY)
        assert broker.queues['schedule_stack.DQ'].qsize() == 1

    @mock.patch('banzai.main.dramatiq.actor', side_effect=actor())
    @mock.patch('banzai.main.should_retry_schedule_stack')
    @mock.patch('banzai.main.schedule_stack.send_with_options', side_effect=Exception)
    @mock.patch('banzai.main.dbs.get_instruments_at_site')
    @mock.patch('banzai.main.lake_utils.get_calibration_blocks_for_time_range')
    @mock.patch('banzai.main.lake_utils.filter_calibration_blocks_for_type')
    def test_schedule_stacking_checks_retry_logic(self, mock_filter_blocks, mock_get_blocks,   mock_get_instruments, mock_schedule_stack, mock_retry_schedule_stack, mock_actor_decorator):
        mock_get_instruments.return_value = [FakeInstrument(site='coj', camera='2m0-SciCam-Spectral',
                                                            enclosure='clma', telescope='2m0a')]
        mock_get_blocks.return_value = fake_blocks_response_json
        mock_filter_blocks.return_value = [fake_blocks_response_json['results'][0]]
        runtime_context = Context({'site': 'coj', 'min_date': '2019-02-19T20:27:49',
                                   'max_date': '2019-02-19T21:55:09', 'db_address': 'db_address',
                                   'frame_type': 'BIAS'})
        with pytest.raises(Exception):
            schedule_stacking_checks(runtime_context)

        print(broker.queues['schedule_stack.DQ'].qsize())
        worker.join()
        mock_retry_schedule_stack.assert_called_with(mock.ANY)
