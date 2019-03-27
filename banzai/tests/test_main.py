import mock
import pytest
import dramatiq

from datetime import datetime, timedelta

from dramatiq.brokers.stub import StubBroker
from dramatiq.actor import Actor
from dramatiq.message import Message

from banzai.tasks import schedule_stacking_checks, schedule_stack, should_retry_schedule_stack
from banzai.settings import CALIBRATION_STACK_DELAYS
from banzai.utils import date_utils
from banzai.context import Context
from banzai.tests.utils import FakeInstrument
from banzai.tests.bias_utils import FakeBiasImage
from banzai.exceptions import IncompleteProcessingException

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
                            }
                        ]
                    }

runtime_context_json = {'site': 'coj', 'min_date': '2019-02-19T20:27:49',
                        'max_date': '2019-02-19T21:55:09', 'frame_type': 'BIAS', 'db_address': 'db_address',
                        'camera': '2m0-SciCam-Spectral', 'enclosure': 'clma', 'telescope': '2m0a'}

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


class TestMain():
    @mock.patch('banzai.tasks.schedule_stack.send_with_options')
    @mock.patch('banzai.tasks.dbs.get_instruments_at_site')
    @mock.patch('banzai.tasks.lake_utils.get_calibration_blocks_for_time_range')
    @mock.patch('banzai.tasks.lake_utils.filter_calibration_blocks_for_type')
    def test_schedule_stacking_checks_queues_task_no_delay(self, mock_filter_blocks, mock_get_blocks,     mock_get_instruments, mock_schedule_stack, stub_broker):
        actor = Actor(fn=foo, broker=stub_broker, actor_name='', queue_name='schedule_stack', priority=1, options={})
        mock_get_instruments.return_value = [FakeInstrument(site='coj', camera='2m0-SciCam-Spectral',
                                                            enclosure='clma', telescope='2m0a')]
        mock_get_blocks.return_value = fake_blocks_response_json
        mock_filter_blocks.return_value = [fake_blocks_response_json['results'][0]]
        mock_schedule_stack.side_effect = actor.send_with_options
        runtime_context = Context(runtime_context_json)
        schedule_stacking_checks(runtime_context)
        mock_schedule_stack.assert_called_with(args=(runtime_context._asdict(), mock_filter_blocks.return_value),
                                               delay=0,
                                               kwargs={'process_any_images': False},
                                               on_failure=mock.ANY)
        assert stub_broker.queues['schedule_stack.DQ'].qsize() == 1

    @mock.patch('banzai.tasks.schedule_stack.send_with_options')
    @mock.patch('banzai.tasks.dbs.get_instruments_at_site')
    @mock.patch('banzai.tasks.lake_utils.get_calibration_blocks_for_time_range')
    @mock.patch('banzai.tasks.lake_utils.filter_calibration_blocks_for_type')
    def test_schedule_stacking_checks_queues_task_with_delay(self, mock_filter_blocks, mock_get_blocks,   mock_get_instruments, mock_schedule_stack, stub_broker):
        actor = Actor(fn=foo, broker=stub_broker, actor_name='', queue_name='schedule_stack', priority=1, options={})
        mock_get_instruments.return_value = [FakeInstrument(site='coj', camera='2m0-SciCam-Spectral',
                                                            enclosure='clma', telescope='2m0a')]
        fake_blocks_response_json['results'][0]['end'] = datetime.strftime(datetime.utcnow() + timedelta(minutes=1), date_utils.TIMESTAMP_FORMAT)
        mock_get_blocks.return_value = fake_blocks_response_json
        mock_filter_blocks.return_value = [fake_blocks_response_json['results'][0]]
        mock_schedule_stack.side_effect = actor.send_with_options
        runtime_context = Context(runtime_context_json)
        schedule_stacking_checks(runtime_context)
        mock_schedule_stack.assert_called_with(args=(runtime_context._asdict(), mock_filter_blocks.return_value),
                                               delay=(60000+CALIBRATION_STACK_DELAYS['BIAS']),
                                               kwargs={'process_any_images': False}, on_failure=mock.ANY)
        assert stub_broker.queues['schedule_stack.DQ'].qsize() == 1

    @mock.patch('banzai.calibrations.process_master_maker')
    @mock.patch('banzai.tasks.dbs.get_individual_calibration_images')
    @mock.patch('banzai.tasks.dbs.query_for_instrument')
    def test_schedule_stack(self, mock_query_for_inst, mock_get_calibration_images, mock_process_master_maker):
        fake_inst = FakeInstrument(site='coj', camera='2m0-SciCam-Spectral', enclosure='clma', telescope='2m0a')
        mock_query_for_inst.return_value = fake_inst
        mock_get_calibration_images.return_value = [FakeBiasImage(), FakeBiasImage()]
        schedule_stack(runtime_context_json, [fake_blocks_response_json['results'][0]])
        mock_process_master_maker.assert_called_with(Context(runtime_context_json), fake_inst, 'BIAS',
                                                     runtime_context_json['min_date'],
                                                     runtime_context_json['max_date'])

    @mock.patch('banzai.calibrations.process_master_maker')
    @mock.patch('banzai.tasks.dbs.get_individual_calibration_images')
    @mock.patch('banzai.tasks.dbs.query_for_instrument')
    def test_schedule_stack_not_enough_images(self, mock_query_for_inst, mock_get_calibration_images, mock_process_master_maker):
        fake_inst = FakeInstrument(site='coj', camera='2m0-SciCam-Spectral', enclosure='clma', telescope='2m0a')
        mock_query_for_inst.return_value = fake_inst
        mock_get_calibration_images.return_value = [FakeBiasImage()]
        with pytest.raises(IncompleteProcessingException) as e:
            schedule_stack(runtime_context_json, [fake_blocks_response_json['results'][0]], process_any_images=False)
        assert e.type is IncompleteProcessingException

    @mock.patch('banzai.tasks.schedule_stack')
    def test_should_retry_schedule_stack_one_retry(self, mock_schedule_stack):
        test_message = Message(options={'retries': 1}, args=('test_arg1', 'test_arg2'),
                               queue_name='schedule_stack', actor_name='', kwargs={})
        return_value = should_retry_schedule_stack(test_message._asdict(), {'type': Exception, 'message': 'test'})
        assert not return_value

    @mock.patch('banzai.tasks.schedule_stack')
    def test_should_retry_schedule_stack_two_retries(self, mock_schedule_stack):
        test_message = Message(options={'retries': 2}, args=('test_arg1', 'test_arg2'),
                               queue_name='schedule_stack', actor_name='', kwargs={})
        should_retry_schedule_stack(test_message._asdict(), {'type': Exception, 'message': 'test'})
        mock_schedule_stack.assert_called_with(*test_message._asdict()['args'], process_any_images=True)
