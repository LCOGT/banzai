"""Unit tests for the smart stacking feature."""
import datetime
import json
from unittest.mock import MagicMock, patch

import pytest
from astropy.io.fits import Header
from celery.exceptions import Retry

from sqlalchemy import text

from banzai import dbs, scheduling
from banzai.stacking import validate_message, check_stack_complete, run_worker_loop, StackingSupervisor
from banzai.scheduling import process_stackframe
from banzai.main import StackframeListener

pytestmark = pytest.mark.smart_stacking


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_address(tmp_path):
    """Create a fresh SQLite DB per test with a site and two instruments."""
    addr = f'sqlite:///{tmp_path}/test.db'
    dbs.create_db(addr, site_deploy=True)
    with dbs.get_session(addr) as session:
        session.add(dbs.Site(id='tst', timezone=0, latitude=0, longitude=0, elevation=0))
        session.add(
            dbs.Instrument(site='tst', camera='cam1', name='cam1', type='1m0-SciCam-Sinistro', nx=4096, ny=4096)
        )
        session.add(
            dbs.Instrument(site='tst', camera='cam2', name='cam2', type='1m0-SciCam-Sinistro', nx=4096, ny=4096)
        )
    return addr


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

class TestDBOperations:

    @staticmethod
    def _upsert(db_address, moluid='mol-001', stack_num=1, frmtotal=5, camera='cam1',
                filepath='/data/frame1.fits', is_last=False, dateobs=None, instrument_enqueue_timestamp=None):
        if dateobs is None:
            dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        dbs.upsert_stack_and_stackframe(
            db_address,
            moluid=moluid,
            stack_num=stack_num,
            frmtotal=frmtotal,
            camera=camera,
            filepath=filepath,
            is_last=is_last,
            dateobs=dateobs,
            instrument_enqueue_timestamp=instrument_enqueue_timestamp,
        )

    def test_upsert_creates_stack_and_stackframe(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        self._upsert(
            db_address, moluid='mol-001', stack_num=1, frmtotal=5,
            camera='cam1', filepath='/data/frame1.fits', is_last=False, dateobs=dateobs,
            instrument_enqueue_timestamp=1771023918500,
        )
        with dbs.get_session(db_address, site_deploy=True) as session:
            stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-001').one()
            stackframes = session.query(dbs.Stackframe).filter(dbs.Stackframe.moluid == 'mol-001').all()

        assert stack.moluid == 'mol-001'
        assert stack.frmtotal == 5
        assert stack.camera == 'cam1'
        assert stack.status == 'active'
        assert stack.completed_at is None
        assert stack.finalize_attempts == 0
        assert stack.next_attempt_at is None
        assert stack.last_preview_count == 0
        assert stack.last_stackframe_at is not None
        assert len(stackframes) == 1
        stackframe = stackframes[0]
        assert stackframe.stack_num == 1
        assert stackframe.filepath == '/data/frame1.fits'
        assert stackframe.dateobs == dateobs
        assert stackframe.is_last is False
        assert stackframe.instrument_enqueue_timestamp == 1771023918500

    def test_upsert_second_stackframe_updates_stack(self, db_address):
        self._upsert(db_address, moluid='mol-update', stack_num=1, frmtotal=5, filepath='/data/frame1.fits')
        with dbs.get_session(db_address, site_deploy=True) as session:
            first_stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-update').one()
            first_last_stackframe_at = first_stack.last_stackframe_at

        self._upsert(db_address, moluid='mol-update', stack_num=2, frmtotal=5, filepath='/data/frame2.fits')

        with dbs.get_session(db_address, site_deploy=True) as session:
            stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-update').one()
            stackframe_count = session.query(dbs.Stackframe).filter(dbs.Stackframe.moluid == 'mol-update').count()

        assert stack.frmtotal == 5
        assert stack.camera == 'cam1'
        assert stack.status == 'active'
        assert stack.last_stackframe_at > first_last_stackframe_at
        assert stackframe_count == 2

    def test_upsert_requeue_resets_terminal_stack(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        self._upsert(
            db_address, moluid='mol-dup', stack_num=1, frmtotal=3,
            camera='cam1', filepath='/data/dup1.fits', is_last=False, dateobs=dateobs,
        )
        dbs.mark_stack_terminal(db_address, 'mol-dup', 'complete')
        dbs.claim_finalize_attempt(db_address, 'mol-dup', [60, 300])
        dbs.set_preview_count(db_address, 'mol-dup', 2)
        with dbs.get_session(db_address, site_deploy=True) as session:
            original_stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-dup').one()
            original_stack_id = original_stack.moluid
            assert original_stack.status == 'complete'
            assert original_stack.completed_at is not None
            assert original_stack.finalize_attempts == 1
            assert original_stack.next_attempt_at is not None

        new_dateobs = datetime.datetime(2024, 6, 15, 13, 0, 0)
        self._upsert(
            db_address, moluid='mol-dup', stack_num=1, frmtotal=3,
            camera='cam1', filepath='/data/dup2.fits', is_last=True, dateobs=new_dateobs,
        )

        with dbs.get_session(db_address, site_deploy=True) as session:
            stacks = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-dup').all()
            stackframes = session.query(dbs.Stackframe).filter(dbs.Stackframe.moluid == 'mol-dup').all()

        assert len(stacks) == 1
        assert stacks[0].moluid == original_stack_id
        assert stacks[0].status == 'active'
        assert stacks[0].completed_at is None
        assert stacks[0].finalize_attempts == 0
        assert stacks[0].next_attempt_at is None
        assert len(stackframes) == 1
        assert stackframes[0].filepath == '/data/dup2.fits'
        assert stackframes[0].is_last is True
        assert stackframes[0].dateobs == new_dateobs

    def test_upsert_requires_filepath(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        with pytest.raises(ValueError, match='filepath is required'):
            self._upsert(
                db_address, moluid='mol-upd', stack_num=1, frmtotal=3,
                camera='cam1', filepath=None, is_last=False, dateobs=dateobs,
            )


# ---------------------------------------------------------------------------
# Status transitions
# ---------------------------------------------------------------------------

class TestStatusTransitions:

    def test_claim_finalize_attempt_increments_and_sets_backoff(self, db_address):
        TestDBOperations._upsert(db_address, moluid='mol-claim')
        before_first_claim = datetime.datetime.utcnow()

        attempt_one = dbs.claim_finalize_attempt(db_address, 'mol-claim', [60, 300])
        attempt_two = dbs.claim_finalize_attempt(db_address, 'mol-claim', [60, 300])
        attempt_three = dbs.claim_finalize_attempt(db_address, 'mol-claim', [60, 300])

        assert (attempt_one, attempt_two, attempt_three) == (1, 2, 3)
        with dbs.get_session(db_address, site_deploy=True) as session:
            stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-claim').one()
        assert stack.finalize_attempts == 3
        assert stack.next_attempt_at >= before_first_claim + datetime.timedelta(seconds=300)

    def test_mark_stack_terminal(self, db_address):
        TestDBOperations._upsert(db_address, moluid='mol-comp')
        dbs.mark_stack_terminal(db_address, 'mol-comp', 'complete')

        with dbs.get_session(db_address, site_deploy=True) as session:
            stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-comp').one()

        assert stack.status == 'complete'
        assert stack.completed_at is not None

    def test_get_active_stacks_filters_by_camera_and_status(self, db_address):
        TestDBOperations._upsert(db_address, moluid='mol-cam1-active', camera='cam1')
        TestDBOperations._upsert(db_address, moluid='mol-cam2-active', camera='cam2')
        TestDBOperations._upsert(db_address, moluid='mol-cam1-complete', camera='cam1')
        dbs.mark_stack_terminal(db_address, 'mol-cam1-complete', 'complete')

        active_cam1 = dbs.get_active_stacks(db_address, 'cam1')

        assert [stack.moluid for stack in active_cam1] == ['mol-cam1-active']

    def test_set_preview_count(self, db_address):
        TestDBOperations._upsert(db_address, moluid='mol-preview')
        dbs.set_preview_count(db_address, 'mol-preview', 7)

        with dbs.get_session(db_address, site_deploy=True) as session:
            stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-preview').one()

        assert stack.last_preview_count == 7


# ---------------------------------------------------------------------------
# Multiple concurrent stacks
# ---------------------------------------------------------------------------

class TestConcurrentStacks:

    def test_get_stackframes_ordered_by_stack_num(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        for stack_num in (3, 1, 2):
            TestDBOperations._upsert(
                db_address, moluid='mol-order', stack_num=stack_num, frmtotal=3,
                camera='cam1', filepath=f'/data/order{stack_num}.fits', is_last=(stack_num == 3), dateobs=dateobs,
            )

        stackframes = dbs.get_stackframes(db_address, 'mol-order')

        assert [stackframe.stack_num for stackframe in stackframes] == [1, 2, 3]

    def test_check_stack_complete_handles_concurrent_stacks_same_camera(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        for i in range(3):
            TestDBOperations._upsert(
                db_address, moluid='mol-A', stack_num=i + 1, frmtotal=3,
                camera='cam1', filepath=f'/data/a{i}.fits', is_last=(i == 2), dateobs=dateobs,
            )
        for i in range(2):
            TestDBOperations._upsert(
                db_address, moluid='mol-B', stack_num=i + 1, frmtotal=5,
                camera='cam1', filepath=f'/data/b{i}.fits', is_last=False, dateobs=dateobs,
            )

        stackframes_a = dbs.get_stackframes(db_address, 'mol-A')
        stackframes_b = dbs.get_stackframes(db_address, 'mol-B')
        assert len(stackframes_a) == 3
        assert len(stackframes_b) == 2
        assert check_stack_complete(stackframes_a, frmtotal=3) is True
        assert check_stack_complete(stackframes_b, frmtotal=5) is False


# ---------------------------------------------------------------------------
# check_stack_complete
# ---------------------------------------------------------------------------

class TestCheckStackComplete:

    @staticmethod
    def _stackframe(filepath='/data/f.fits', is_last=False):
        f = MagicMock()
        f.filepath = filepath
        f.is_last = is_last
        return f

    def test_check_stack_complete_all_stackframes_arrived(self):
        stackframes = [self._stackframe() for _ in range(3)]
        assert check_stack_complete(stackframes, frmtotal=3) is True

    def test_check_stack_complete_partial_without_is_last(self):
        stackframes = [self._stackframe() for _ in range(3)]
        assert check_stack_complete(stackframes, frmtotal=5) is False

    def test_check_stack_complete_partial_with_is_last(self):
        stackframes = [self._stackframe() for _ in range(2)] + [self._stackframe(is_last=True)]
        assert check_stack_complete(stackframes, frmtotal=5) is True

    def test_check_stack_complete_empty_stackframes(self):
        assert check_stack_complete([], frmtotal=5) is False

    def test_check_stack_complete_empty_stackframes_with_zero_total(self):
        assert check_stack_complete([], frmtotal=0) is False


# ---------------------------------------------------------------------------
# Retention / cleanup
# ---------------------------------------------------------------------------

class TestRetention:

    def test_cleanup_old_stacks_deletes_stack_and_stackframes(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        for stack_num in range(1, 4):
            TestDBOperations._upsert(
                db_address, moluid='mol-old', stack_num=stack_num, frmtotal=3,
                camera='cam1', filepath=f'/data/old{stack_num}.fits', is_last=(stack_num == 3), dateobs=dateobs,
            )
        for stack_num in range(1, 4):
            TestDBOperations._upsert(
                db_address, moluid='mol-active-old', stack_num=stack_num, frmtotal=3,
                camera='cam1', filepath=f'/data/active{stack_num}.fits',
                is_last=(stack_num == 3), dateobs=dateobs,
            )
        dbs.mark_stack_terminal(db_address, 'mol-old', 'complete')

        with dbs.get_session(db_address) as session:
            old_date = datetime.datetime.utcnow() - datetime.timedelta(days=30)
            session.execute(
                text("UPDATE stacks SET completed_at = :old_date WHERE moluid = :moluid"),
                {'old_date': old_date, 'moluid': 'mol-old'},
            )
            session.execute(
                text("UPDATE stacks SET completed_at = :old_date WHERE moluid = :moluid"),
                {'old_date': old_date, 'moluid': 'mol-active-old'},
            )

        dbs.cleanup_old_stacks(db_address, retention_days=7)

        with dbs.get_session(db_address, site_deploy=True) as session:
            old_stack_count = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-old').count()
            old_frame_count = session.query(dbs.Stackframe).filter(dbs.Stackframe.moluid == 'mol-old').count()
            active_stack_count = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-active-old').count()
            active_frame_count = session.query(dbs.Stackframe).filter(dbs.Stackframe.moluid == 'mol-active-old').count()

        assert old_stack_count == 0
        assert old_frame_count == 0
        assert active_stack_count == 1
        assert active_frame_count == 3


# ---------------------------------------------------------------------------
# StackframeListener on_message
# ---------------------------------------------------------------------------

class TestStackframeListenerOnMessage:
    """on_message dispatches to Celery; no FITS I/O or DB work here."""

    @patch('banzai.main.process_stackframe')
    def test_on_message_dispatches_valid(self, mock_task):
        ctx = MagicMock(STACKFRAME_TASK_QUEUE_NAME='stackframe_tasks')
        listener = StackframeListener(ctx)

        body = {
            'fits_file': '/path/to/frame.fits',
            'last_frame': False,
            'instrument_enqueue_timestamp': 1771023918500,
        }
        mock_message = MagicMock()

        listener.on_message(body, mock_message)

        mock_task.apply_async.assert_called_once_with(
            args=(body, vars(ctx)),
            queue='stackframe_tasks',
        )
        mock_message.ack.assert_called_once()

    @patch('banzai.main.process_stackframe')
    def test_on_message_parses_json_string(self, mock_task):
        ctx = MagicMock(STACKFRAME_TASK_QUEUE_NAME='stackframe_tasks')
        listener = StackframeListener(ctx)

        body = {
            'fits_file': '/path/to/frame.fits',
            'last_frame': False,
            'instrument_enqueue_timestamp': 1771023918500,
        }
        mock_message = MagicMock()

        listener.on_message(json.dumps(body), mock_message)

        mock_task.apply_async.assert_called_once_with(
            args=(body, vars(ctx)),
            queue='stackframe_tasks',
        )
        mock_message.ack.assert_called_once()

    @patch('banzai.main.process_stackframe')
    def test_on_message_parses_json_bytes(self, mock_task):
        ctx = MagicMock(STACKFRAME_TASK_QUEUE_NAME='stackframe_tasks')
        listener = StackframeListener(ctx)

        body = {
            'fits_file': '/path/to/frame.fits',
            'last_frame': False,
            'instrument_enqueue_timestamp': 1771023918500,
        }
        mock_message = MagicMock()

        listener.on_message(json.dumps(body).encode('utf-8'), mock_message)

        mock_task.apply_async.assert_called_once_with(
            args=(body, vars(ctx)),
            queue='stackframe_tasks',
        )
        mock_message.ack.assert_called_once()

    @patch('banzai.main.process_stackframe')
    def test_on_message_invalid_no_dispatch(self, mock_task):
        listener = StackframeListener(MagicMock())

        body = {
            'last_frame': True,
            # missing fits_file and instrument_enqueue_timestamp
        }
        mock_message = MagicMock()

        listener.on_message(body, mock_message)

        mock_task.apply_async.assert_not_called()
        mock_message.ack.assert_called_once()

    @patch('banzai.main.process_stackframe')
    def test_on_message_malformed_json_acks_and_no_dispatch(self, mock_task):
        ctx = MagicMock(STACKFRAME_TASK_QUEUE_NAME='stackframe_tasks')
        listener = StackframeListener(ctx)
        mock_message = MagicMock()

        listener.on_message('{not valid json}', mock_message)

        mock_task.apply_async.assert_not_called()
        mock_message.ack.assert_called_once()

    @patch('banzai.main.process_stackframe')
    def test_on_message_invalid_utf8_acks_and_no_dispatch(self, mock_task):
        ctx = MagicMock(STACKFRAME_TASK_QUEUE_NAME='stackframe_tasks')
        listener = StackframeListener(ctx)
        mock_message = MagicMock()

        listener.on_message(b'\xff', mock_message)

        mock_task.apply_async.assert_not_called()
        mock_message.ack.assert_called_once()

    @patch('banzai.main.process_stackframe')
    def test_on_message_non_object_json_acks_and_no_dispatch(self, mock_task):
        ctx = MagicMock(STACKFRAME_TASK_QUEUE_NAME='stackframe_tasks')
        listener = StackframeListener(ctx)
        mock_message = MagicMock()

        listener.on_message(json.dumps(['not', 'an', 'object']), mock_message)

        mock_task.apply_async.assert_not_called()
        mock_message.ack.assert_called_once()


# ---------------------------------------------------------------------------
# process_stackframe Celery task
# ---------------------------------------------------------------------------

class TestProcessStackframe:
    """Test the Celery task that does the actual stackframe processing."""

    @staticmethod
    def _make_fits_header(**overrides):
        """Build a FITS header with the standard stack keys."""
        h = Header()
        h['INSTRUME'] = 'cam1'
        h['DATE-OBS'] = '2024-01-01T00:00:00'
        h['STACK'] = 'T'
        h['MOLFRNUM'] = 1
        h['FRMTOTAL'] = 5
        h['MOLUID'] = 'mol-xyz'
        for k, v in overrides.items():
            h[k] = v
        return h

    @staticmethod
    def _make_mock_image(output_dir='/data/processed', output_filename='frame-e09.fits'):
        """Build a mock image returned by run_pipeline_stages."""
        img = MagicMock()
        img.get_output_directory.return_value = output_dir
        img.get_output_filename.return_value = output_filename
        return img

    @pytest.mark.parametrize('last_frame_val, expected_is_last', [
        (False, False),
        (True, True),
    ])
    @patch('banzai.scheduling.stage_utils.run_pipeline_stages')
    def test_process_stackframe_upserts_stackframe_and_does_not_notify_redis(
            self, mock_run_stages, last_frame_val, expected_is_last, db_address, monkeypatch):
        mock_image = self._make_mock_image()
        mock_run_stages.return_value = [mock_image]
        mock_upsert = MagicMock()
        redis_module = MagicMock()
        monkeypatch.setattr(scheduling, 'upsert_stack_and_stackframe', mock_upsert, raising=False)
        monkeypatch.setattr(scheduling, 'redis', redis_module, raising=False)

        header = self._make_fits_header()
        body = {
            'fits_file': '/path/to/frame.fits',
            'last_frame': last_frame_val,
            'instrument_enqueue_timestamp': 1771023918500,
        }
        runtime_context = {'db_address': db_address, 'REDIS_URL': 'redis://localhost:6379/0'}

        with patch('banzai.scheduling.fits_utils.get_primary_header', return_value=header):
            process_stackframe(body, runtime_context)

        mock_run_stages.assert_called_once()
        mock_upsert.assert_called_once_with(
            db_address,
            moluid='mol-xyz',
            stack_num=1,
            frmtotal=5,
            camera='cam1',
            filepath='/data/processed/frame-e09.fits',
            is_last=expected_is_last,
            dateobs=datetime.datetime(2024, 1, 1, 0, 0, 0),
            instrument_enqueue_timestamp=1771023918500,
        )
        redis_module.Redis.from_url.assert_not_called()

    @patch('banzai.scheduling.stage_utils.run_pipeline_stages')
    def test_process_stackframe_upserts_only_after_reduction(self, mock_run_stages, db_address, monkeypatch):
        mock_upsert = MagicMock()
        redis_module = MagicMock()
        monkeypatch.setattr(scheduling, 'upsert_stack_and_stackframe', mock_upsert, raising=False)
        monkeypatch.setattr(scheduling, 'redis', redis_module, raising=False)
        mock_image = self._make_mock_image()

        def _run_stages(*args, **kwargs):
            mock_upsert.assert_not_called()
            return [mock_image]

        mock_run_stages.side_effect = _run_stages
        header = self._make_fits_header()
        body = {
            'fits_file': '/path/to/frame.fits',
            'last_frame': False,
            'instrument_enqueue_timestamp': 1771023918500,
        }
        runtime_context = {'db_address': db_address, 'REDIS_URL': 'redis://localhost:6379/0'}

        with patch('banzai.scheduling.fits_utils.get_primary_header', return_value=header):
            process_stackframe(body, runtime_context)

        mock_upsert.assert_called_once()
        assert mock_upsert.call_args.kwargs['filepath'] == '/data/processed/frame-e09.fits'
        redis_module.Redis.from_url.assert_not_called()

    @patch('banzai.scheduling.stage_utils.run_pipeline_stages')
    def test_process_stackframe_does_not_upsert_or_notify_without_reduced_image(
            self, mock_run_stages, db_address, monkeypatch):
        mock_upsert = MagicMock()
        redis_module = MagicMock()
        monkeypatch.setattr(scheduling, 'upsert_stack_and_stackframe', mock_upsert, raising=False)
        monkeypatch.setattr(scheduling, 'redis', redis_module, raising=False)
        mock_run_stages.return_value = []
        header = self._make_fits_header()
        body = {
            'fits_file': '/path/to/frame.fits',
            'last_frame': False,
            'instrument_enqueue_timestamp': 1771023918500,
        }
        runtime_context = {'db_address': db_address, 'REDIS_URL': 'redis://localhost:6379/0'}

        with patch('banzai.scheduling.fits_utils.get_primary_header', return_value=header):
            process_stackframe(body, runtime_context)

        mock_upsert.assert_not_called()
        redis_module.Redis.from_url.assert_not_called()

    def test_process_stackframe_retries_on_unreadable_header(self, db_address):
        """If get_primary_header returns None (I/O error), the task must retry, not swallow the failure."""
        body = {
            'fits_file': '/path/to/corrupt.fits',
            'last_frame': True,
            'instrument_enqueue_timestamp': 1771023918500,
        }
        runtime_context = {'db_address': db_address, 'REDIS_URL': 'redis://localhost:6379/0'}

        with patch('banzai.scheduling.fits_utils.get_primary_header', return_value=None), \
             patch.object(process_stackframe, 'retry', side_effect=Retry()) as mock_retry:
            with pytest.raises(Retry):
                process_stackframe(body, runtime_context)

        mock_retry.assert_called_once()


# ---------------------------------------------------------------------------
# Supervisor
# ---------------------------------------------------------------------------

class TestSupervisor:

    @patch('banzai.stacking.dbs.get_instruments_at_site',
           return_value=[MagicMock(camera='cam1'), MagicMock(camera='cam2'), MagicMock(camera='cam3')])
    @patch('banzai.stacking.multiprocessing.Process')
    def test_stacking_supervisor_spawns_per_camera(self, mock_process_cls, mock_discover):
        supervisor = StackingSupervisor(
            site_id='tst',
            db_address='sqlite:///fake.db',
            redis_url='redis://localhost:6379',
        )
        supervisor.start()
        assert mock_process_cls.call_count == 3
        assert mock_process_cls.return_value.start.call_count == 3


# ---------------------------------------------------------------------------
# Worker loop resilience
# ---------------------------------------------------------------------------

class TestWorkerLoopResilience:

    @patch('banzai.stacking.redis_lib.Redis.from_url')
    @patch('banzai.stacking.time.sleep')
    @patch('banzai.stacking.process_notifications')
    def test_run_worker_loop_continues_after_exception(self, mock_process, mock_sleep, mock_redis_cls):
        """run_worker_loop must not crash when process_notifications raises; it should log and continue."""
        # First call raises, second call raises KeyboardInterrupt to escape the infinite loop.
        mock_process.side_effect = [Exception('boom'), KeyboardInterrupt]
        with pytest.raises(KeyboardInterrupt):
            run_worker_loop('cam1', 'sqlite:///fake.db', 'redis://localhost:6379', poll_interval=0)
        # process_notifications was called twice: once raised Exception, once raised KeyboardInterrupt.
        assert mock_process.call_count == 2
        # sleep should have been called once (after the transient Exception, before continuing).
        mock_sleep.assert_called_once_with(0)
