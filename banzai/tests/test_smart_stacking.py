"""Unit tests for the smart stacking feature."""
import datetime
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from astropy.io.fits import Header
from celery.exceptions import Retry

from sqlalchemy import text

from banzai import dbs, scheduling
from banzai.stacking import (check_stack_complete, stack_has_timed_out, finalize_stack,
                             process_camera_tick, run_worker_loop, run_supervisor,
                             FINALIZE_BACKOFF_SECONDS, MAX_FINALIZE_ATTEMPTS)
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
                filepath='/data/frame1.fits', is_last=False, dateobs=None):
        if dateobs is None:
            dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        return dbs.upsert_stack_and_stackframe(
            db_address,
            moluid=moluid,
            stack_num=stack_num,
            frmtotal=frmtotal,
            camera=camera,
            filepath=filepath,
            is_last=is_last,
            dateobs=dateobs,
        )

    def test_upsert_returns_created_flag(self, db_address):
        assert self._upsert(db_address, moluid='mol-created', stack_num=1) is True
        assert self._upsert(db_address, moluid='mol-created', stack_num=2) is False

    def test_upsert_creates_stack_and_stackframe(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        self._upsert(
            db_address, moluid='mol-001', stack_num=1, frmtotal=5,
            camera='cam1', filepath='/data/frame1.fits', is_last=False, dateobs=dateobs,
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

    def test_upsert_second_stackframe_updates_stack(self, db_address):
        self._upsert(db_address, moluid='mol-update', stack_num=1, frmtotal=5, filepath='/data/frame1.fits')
        with dbs.get_session(db_address, site_deploy=True) as session:
            first_stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-update').one()
            first_last_stackframe_at = first_stack.last_stackframe_at

        dbs.set_preview_count(db_address, 'mol-update', 1)
        self._upsert(db_address, moluid='mol-update', stack_num=2, frmtotal=5, filepath='/data/frame2.fits')

        with dbs.get_session(db_address, site_deploy=True) as session:
            stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-update').one()
            stackframe_count = session.query(dbs.Stackframe).filter(dbs.Stackframe.moluid == 'mol-update').count()

        assert stack.frmtotal == 5
        assert stack.camera == 'cam1'
        assert stack.status == 'active'
        assert stack.last_preview_count == 1
        assert stack.last_stackframe_at > first_last_stackframe_at
        assert stackframe_count == 2

    @pytest.mark.parametrize('terminal_status', ['complete', 'error'])
    def test_upsert_requeue_resets_terminal_stack(self, db_address, terminal_status):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        self._upsert(
            db_address, moluid='mol-dup', stack_num=1, frmtotal=3,
            camera='cam1', filepath='/data/dup1.fits', is_last=False, dateobs=dateobs,
        )
        dbs.mark_stack_terminal(db_address, 'mol-dup', terminal_status)
        dbs.claim_finalize_attempt(db_address, 'mol-dup', [60, 300])
        dbs.set_preview_count(db_address, 'mol-dup', 2)
        with dbs.get_session(db_address, site_deploy=True) as session:
            original_stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-dup').one()
            original_stack_id = original_stack.moluid
            assert original_stack.status == terminal_status
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
        assert stacks[0].last_preview_count == 0
        assert len(stackframes) == 1
        assert stackframes[0].filepath == '/data/dup2.fits'
        assert stackframes[0].is_last is True
        assert stackframes[0].dateobs == new_dateobs

    @pytest.mark.parametrize('late_stack_num', [1, 2], ids=['replay', 'new-member'])
    def test_upsert_ignores_stackframe_after_timeout(self, db_address, late_stack_num):
        original_dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        self._upsert(
            db_address, moluid='mol-timeout', stack_num=1, frmtotal=3,
            camera='cam1', filepath='/data/original.fits', is_last=False, dateobs=original_dateobs,
        )
        dbs.claim_finalize_attempt(db_address, 'mol-timeout', [60, 300])
        dbs.set_preview_count(db_address, 'mol-timeout', 1)
        dbs.mark_stack_terminal(db_address, 'mol-timeout', 'timeout')

        with dbs.get_session(db_address, site_deploy=True) as session:
            original_stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-timeout').one()
            original_frame = session.query(dbs.Stackframe).filter(dbs.Stackframe.moluid == 'mol-timeout').one()
            original_stack_state = (
                original_stack.camera, original_stack.frmtotal, original_stack.status,
                original_stack.last_stackframe_at, original_stack.completed_at,
                original_stack.finalize_attempts, original_stack.next_attempt_at,
                original_stack.last_preview_count,
            )
            original_frame_state = (
                original_frame.filepath, original_frame.dateobs, original_frame.is_last,
                original_frame.created_at,
            )

        result = self._upsert(
            db_address, moluid='mol-timeout', stack_num=late_stack_num, frmtotal=99,
            camera='cam2', filepath='/data/late.fits', is_last=True,
            dateobs=datetime.datetime(2024, 6, 15, 13, 0, 0),
        )

        with dbs.get_session(db_address, site_deploy=True) as session:
            stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-timeout').one()
            stackframes = session.query(dbs.Stackframe).filter(dbs.Stackframe.moluid == 'mol-timeout').all()

        assert result is None
        assert (
            stack.camera, stack.frmtotal, stack.status, stack.last_stackframe_at, stack.completed_at,
            stack.finalize_attempts, stack.next_attempt_at, stack.last_preview_count,
        ) == original_stack_state
        assert len(stackframes) == 1
        assert (
            stackframes[0].filepath, stackframes[0].dateobs, stackframes[0].is_last,
            stackframes[0].created_at,
        ) == original_frame_state
        assert dbs.get_active_stacks(db_address, 'cam1') == []

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

    def test_cleanup_old_stacks_retains_timeout_tombstone(self, db_address):
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
        for stack_num in range(1, 3):
            TestDBOperations._upsert(
                db_address, moluid='mol-timeout-old', stack_num=stack_num, frmtotal=3,
                camera='cam1', filepath=f'/data/timeout{stack_num}.fits',
                is_last=False, dateobs=dateobs,
            )
        dbs.mark_stack_terminal(db_address, 'mol-old', 'complete')
        dbs.mark_stack_terminal(db_address, 'mol-timeout-old', 'timeout')

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
            session.execute(
                text("UPDATE stacks SET completed_at = :old_date WHERE moluid = :moluid"),
                {'old_date': old_date, 'moluid': 'mol-timeout-old'},
            )

        dbs.cleanup_old_stacks(db_address, retention_days=7)

        with dbs.get_session(db_address, site_deploy=True) as session:
            old_stack_count = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-old').count()
            old_frame_count = session.query(dbs.Stackframe).filter(dbs.Stackframe.moluid == 'mol-old').count()
            active_stack_count = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-active-old').count()
            active_frame_count = session.query(dbs.Stackframe).filter(dbs.Stackframe.moluid == 'mol-active-old').count()
            timeout_stack = session.query(dbs.Stack).filter(dbs.Stack.moluid == 'mol-timeout-old').one()
            timeout_frame_count = session.query(dbs.Stackframe).filter(
                dbs.Stackframe.moluid == 'mol-timeout-old').count()

        assert old_stack_count == 0
        assert old_frame_count == 0
        assert active_stack_count == 1
        assert active_frame_count == 3
        assert timeout_stack.status == 'timeout'
        assert timeout_frame_count == 0
        assert TestDBOperations._upsert(
            db_address, moluid='mol-timeout-old', stack_num=3, frmtotal=3,
            camera='cam1', filepath='/data/late.fits', is_last=True, dateobs=dateobs,
        ) is None


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
            # Deployed producers may continue sending this ignored extra field.
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
            # missing fits_file
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

    @patch('banzai.scheduling.logger')
    @patch('banzai.scheduling.stage_utils.run_pipeline_stages')
    def test_process_stackframe_logs_timeout_rejection(self, mock_run_stages, mock_logger,
                                                       db_address, monkeypatch):
        mock_run_stages.return_value = [self._make_mock_image()]
        monkeypatch.setattr(scheduling, 'upsert_stack_and_stackframe', MagicMock(return_value=None))
        header = self._make_fits_header()

        with patch('banzai.scheduling.fits_utils.get_primary_header', return_value=header):
            process_stackframe({'fits_file': '/path/to/frame.fits'}, {'db_address': db_address})

        mock_logger.info.assert_called_once_with(
            'Ignored reduced stackframe for timed-out smartstack',
            extra_tags={'smartstack_event': 'frame_ignored',
                        'smartstack_moluid': 'mol-xyz',
                        'smartstack_camera': 'cam1',
                        'smartstack_stack_num': 1,
                        'smartstack_filepath': '/data/processed/frame-e09.fits',
                        'smartstack_status': 'timeout'},
        )

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
        }
        runtime_context = {'db_address': db_address, 'REDIS_URL': 'redis://localhost:6379/0'}

        with patch('banzai.scheduling.fits_utils.get_primary_header', return_value=None), \
             patch.object(process_stackframe, 'retry', side_effect=Retry()) as mock_retry:
            with pytest.raises(Retry):
                process_stackframe(body, runtime_context)

        mock_retry.assert_called_once()


# ---------------------------------------------------------------------------
# Worker: finalize / preview / timeout helpers
# ---------------------------------------------------------------------------

def _runtime_context(**overrides):
    """Fake runtime context exposing exactly the attributes the worker reads."""
    defaults = dict(
        db_address='sqlite:///fake.db',
        broker_url='amqp://localhost:5672',
        SHIPPER_EXCHANGE='ship_files',
        SHIPPER_QUEUE_NAME='ship',
        SMARTSTACK_PREVIEWS=True,
        stack_timeout_minutes=20,
        stack_retention_days=30,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _stack(moluid='mol-w', frmtotal=3, finalize_attempts=0, next_attempt_at=None,
           last_preview_count=0, last_stackframe_at=None):
    if last_stackframe_at is None:
        last_stackframe_at = datetime.datetime.utcnow()
    return SimpleNamespace(
        moluid=moluid, camera='cam1', frmtotal=frmtotal, status='active',
        finalize_attempts=finalize_attempts, next_attempt_at=next_attempt_at,
        last_preview_count=last_preview_count, last_stackframe_at=last_stackframe_at,
    )


def _frame(stack_num, is_last=False, instrument_enqueue_timestamp=None, filepath=None):
    return SimpleNamespace(
        stack_num=stack_num, is_last=is_last,
        instrument_enqueue_timestamp=instrument_enqueue_timestamp,
        filepath=filepath or f'/data/f{stack_num}.fits',
    )


class TestFinalizeStack:

    def test_finalize_claims_before_work(self):
        """The attempt is claimed first, then run_final -> publish -> mark_terminal, in that order."""
        rc = _runtime_context()
        stack = _stack(finalize_attempts=0, next_attempt_at=None)
        stackframes = [_frame(1, instrument_enqueue_timestamp=111),
                       _frame(2, is_last=True, instrument_enqueue_timestamp=222)]
        with patch('banzai.stacking.dbs') as mock_dbs, \
             patch('banzai.stacking.smartstack_products') as mock_products, \
             patch('banzai.stacking.post_to_shipper_queue') as mock_publish:
            mock_products.run_final.return_value = ('/o/e45.fits', '/o/small.jpg', '/o/large.jpg')
            manager = MagicMock()
            manager.attach_mock(mock_dbs.claim_finalize_attempt, 'claim')
            manager.attach_mock(mock_products.run_final, 'run_final')
            manager.attach_mock(mock_publish, 'publish')
            manager.attach_mock(mock_dbs.mark_stack_terminal, 'mark_terminal')

            finalize_stack(rc, stack, stackframes, 'complete')

        assert [call[0] for call in manager.mock_calls] == ['claim', 'run_final', 'publish', 'mark_terminal']
        mock_dbs.claim_finalize_attempt.assert_called_once_with(rc.db_address, stack.moluid, FINALIZE_BACKOFF_SECONDS)
        mock_publish.assert_called_once_with(
            rc.broker_url, rc.SHIPPER_EXCHANGE, rc.SHIPPER_QUEUE_NAME,
            fits_path='/o/e45.fits', small_thumbnail='/o/small.jpg', large_thumbnail='/o/large.jpg',
            instrument_enqueue_timestamp=222,
        )
        mock_dbs.mark_stack_terminal.assert_called_once_with(rc.db_address, stack.moluid, 'complete')

    def test_finalize_failure_leaves_stack_active(self):
        """run_final raising: attempt already burned by claim, but no mark_terminal and no exception escapes."""
        rc = _runtime_context()
        stack = _stack()
        stackframes = [_frame(1, instrument_enqueue_timestamp=111)]
        with patch('banzai.stacking.dbs') as mock_dbs, \
             patch('banzai.stacking.smartstack_products') as mock_products, \
             patch('banzai.stacking.post_to_shipper_queue') as mock_publish:
            mock_products.run_final.side_effect = RuntimeError('stack blew up')

            finalize_stack(rc, stack, stackframes, 'complete')

        mock_dbs.claim_finalize_attempt.assert_called_once()
        mock_publish.assert_not_called()
        mock_dbs.mark_stack_terminal.assert_not_called()

    def test_publish_failure_leaves_stack_active(self):
        """post_to_shipper_queue raising: the stack is not marked terminal (publish-before-mark)."""
        rc = _runtime_context()
        stack = _stack()
        stackframes = [_frame(1, instrument_enqueue_timestamp=111)]
        with patch('banzai.stacking.dbs') as mock_dbs, \
             patch('banzai.stacking.smartstack_products') as mock_products, \
             patch('banzai.stacking.post_to_shipper_queue') as mock_publish:
            mock_products.run_final.return_value = ('/o/e45.fits', '/o/small.jpg', '/o/large.jpg')
            mock_publish.side_effect = RuntimeError('broker down')

            finalize_stack(rc, stack, stackframes, 'complete')

        mock_dbs.claim_finalize_attempt.assert_called_once()
        mock_products.run_final.assert_called_once()
        mock_dbs.mark_stack_terminal.assert_not_called()

    def test_exhausted_attempts_marks_error(self):
        """At/over MAX attempts, the stack is marked 'error' without claiming or doing any work."""
        rc = _runtime_context()
        stack = _stack(finalize_attempts=MAX_FINALIZE_ATTEMPTS, next_attempt_at=None)
        stackframes = [_frame(1, instrument_enqueue_timestamp=111)]
        with patch('banzai.stacking.dbs') as mock_dbs, \
             patch('banzai.stacking.smartstack_products') as mock_products, \
             patch('banzai.stacking.post_to_shipper_queue') as mock_publish:

            finalize_stack(rc, stack, stackframes, 'complete')

        mock_dbs.mark_stack_terminal.assert_called_once_with(rc.db_address, stack.moluid, 'error')
        mock_dbs.claim_finalize_attempt.assert_not_called()
        mock_products.run_final.assert_not_called()
        mock_publish.assert_not_called()


class TestStackTimedOut:

    def test_stack_has_timed_out_boundary(self):
        now = datetime.datetime(2024, 6, 15, 12, 0, 0)
        stale = _stack(last_stackframe_at=now - datetime.timedelta(minutes=21))
        fresh = _stack(last_stackframe_at=now - datetime.timedelta(minutes=19))
        assert stack_has_timed_out(stale, 20, now=now) is True
        assert stack_has_timed_out(fresh, 20, now=now) is False


class TestProcessCameraTick:

    def test_backoff_window_respected(self):
        """A terminal stack whose next_attempt_at is in the future is not finalized this tick."""
        rc = _runtime_context()
        now = datetime.datetime.utcnow()
        stack = _stack(frmtotal=3, finalize_attempts=1, next_attempt_at=now + datetime.timedelta(minutes=5))
        stackframes = [_frame(i, is_last=(i == 3)) for i in (1, 2, 3)]
        with patch('banzai.stacking.dbs') as mock_dbs, \
             patch('banzai.stacking.finalize_stack') as mock_finalize, \
             patch('banzai.stacking.smartstack_products') as mock_products:
            mock_dbs.get_active_stacks.return_value = [stack]
            mock_dbs.get_stackframes.return_value = stackframes

            process_camera_tick(rc, 'cam1')

        mock_finalize.assert_not_called()
        mock_products.run_preview.assert_not_called()

    def test_timeout_finalizes_partial(self):
        """A partial stack past the cadence timeout is finalized with status 'timeout'."""
        rc = _runtime_context(stack_timeout_minutes=20)
        stack = _stack(frmtotal=5, last_stackframe_at=datetime.datetime.utcnow() - datetime.timedelta(minutes=21))
        stackframes = [_frame(1), _frame(2)]
        with patch('banzai.stacking.dbs') as mock_dbs, \
             patch('banzai.stacking.finalize_stack') as mock_finalize:
            mock_dbs.get_active_stacks.return_value = [stack]
            mock_dbs.get_stackframes.return_value = stackframes

            process_camera_tick(rc, 'cam1')

        mock_finalize.assert_called_once_with(rc, stack, stackframes, 'timeout')

    def test_fresh_stackframe_resets_timeout_clock(self):
        """A fresh arrival keeps the stack active: no timeout finalize."""
        rc = _runtime_context(stack_timeout_minutes=20, SMARTSTACK_PREVIEWS=False)
        stack = _stack(frmtotal=5, last_stackframe_at=datetime.datetime.utcnow() - datetime.timedelta(minutes=5))
        stackframes = [_frame(1), _frame(2)]
        with patch('banzai.stacking.dbs') as mock_dbs, \
             patch('banzai.stacking.finalize_stack') as mock_finalize:
            mock_dbs.get_active_stacks.return_value = [stack]
            mock_dbs.get_stackframes.return_value = stackframes

            process_camera_tick(rc, 'cam1')

        mock_finalize.assert_not_called()

    def test_complete_wins_over_timeout(self):
        """A stack that is both complete and past the timeout finalizes as 'complete'."""
        rc = _runtime_context(stack_timeout_minutes=20)
        stack = _stack(frmtotal=3, last_stackframe_at=datetime.datetime.utcnow() - datetime.timedelta(minutes=30))
        stackframes = [_frame(i, is_last=(i == 3)) for i in (1, 2, 3)]
        with patch('banzai.stacking.dbs') as mock_dbs, \
             patch('banzai.stacking.finalize_stack') as mock_finalize:
            mock_dbs.get_active_stacks.return_value = [stack]
            mock_dbs.get_stackframes.return_value = stackframes

            process_camera_tick(rc, 'cam1')

        mock_finalize.assert_called_once_with(rc, stack, stackframes, 'complete')

    def test_complete_stack_never_previews(self):
        """A complete stack takes the finalize path even if the preview count would grow."""
        rc = _runtime_context(SMARTSTACK_PREVIEWS=True)
        stack = _stack(frmtotal=3, last_preview_count=0)
        stackframes = [_frame(i, is_last=(i == 3)) for i in (1, 2, 3)]
        with patch('banzai.stacking.dbs') as mock_dbs, \
             patch('banzai.stacking.finalize_stack') as mock_finalize, \
             patch('banzai.stacking.smartstack_products') as mock_products:
            mock_dbs.get_active_stacks.return_value = [stack]
            mock_dbs.get_stackframes.return_value = stackframes

            process_camera_tick(rc, 'cam1')

        mock_finalize.assert_called_once_with(rc, stack, stackframes, 'complete')
        mock_products.run_preview.assert_not_called()
        mock_dbs.set_preview_count.assert_not_called()

    def test_preview_only_when_count_grew(self):
        """Only stacks whose stackframe count exceeds last_preview_count get a new preview."""
        rc = _runtime_context(SMARTSTACK_PREVIEWS=True)
        grown = _stack(moluid='grown', frmtotal=5, last_preview_count=2)
        same = _stack(moluid='same', frmtotal=5, last_preview_count=3)
        frames_by_mol = {
            'grown': [_frame(1), _frame(2), _frame(3)],
            'same': [_frame(1), _frame(2), _frame(3)],
        }
        with patch('banzai.stacking.dbs') as mock_dbs, \
             patch('banzai.stacking.finalize_stack') as mock_finalize, \
             patch('banzai.stacking.smartstack_products') as mock_products:
            mock_dbs.get_active_stacks.return_value = [grown, same]
            mock_dbs.get_stackframes.side_effect = lambda db_address, moluid: frames_by_mol[moluid]

            process_camera_tick(rc, 'cam1')

        mock_finalize.assert_not_called()
        mock_products.run_preview.assert_called_once_with(frames_by_mol['grown'], rc, 'grown')
        mock_dbs.set_preview_count.assert_called_once_with(rc.db_address, 'grown', 3)

    def test_preview_waits_for_stackframe_one(self):
        """Do not publish a preview whose basename could change when stackframe 1 arrives."""
        rc = _runtime_context(SMARTSTACK_PREVIEWS=True)
        stack = _stack(frmtotal=3, last_preview_count=0)
        stackframes = [_frame(2)]
        with patch('banzai.stacking.dbs') as mock_dbs, \
             patch('banzai.stacking.finalize_stack') as mock_finalize, \
             patch('banzai.stacking.smartstack_products') as mock_products:
            mock_dbs.get_active_stacks.return_value = [stack]
            mock_dbs.get_stackframes.return_value = stackframes

            process_camera_tick(rc, 'cam1')

        mock_finalize.assert_not_called()
        mock_products.run_preview.assert_not_called()
        mock_dbs.set_preview_count.assert_not_called()

    def test_preview_failure_does_not_update_count(self):
        """If run_preview raises, the preview count is not advanced and the tick keeps going."""
        rc = _runtime_context(SMARTSTACK_PREVIEWS=True)
        stack = _stack(frmtotal=5, last_preview_count=2)
        stackframes = [_frame(1), _frame(2), _frame(3)]
        with patch('banzai.stacking.dbs') as mock_dbs, \
             patch('banzai.stacking.finalize_stack') as mock_finalize, \
             patch('banzai.stacking.smartstack_products') as mock_products:
            mock_dbs.get_active_stacks.return_value = [stack]
            mock_dbs.get_stackframes.return_value = stackframes
            mock_products.run_preview.side_effect = RuntimeError('render failed')

            process_camera_tick(rc, 'cam1')

        mock_products.run_preview.assert_called_once()
        mock_dbs.set_preview_count.assert_not_called()
        mock_finalize.assert_not_called()

    def test_previews_disabled_kill_switch(self):
        """With SMARTSTACK_PREVIEWS off, a growing active stack renders no preview."""
        rc = _runtime_context(SMARTSTACK_PREVIEWS=False)
        stack = _stack(frmtotal=5, last_preview_count=0)
        stackframes = [_frame(1), _frame(2), _frame(3)]
        with patch('banzai.stacking.dbs') as mock_dbs, \
             patch('banzai.stacking.finalize_stack') as mock_finalize, \
             patch('banzai.stacking.smartstack_products') as mock_products:
            mock_dbs.get_active_stacks.return_value = [stack]
            mock_dbs.get_stackframes.return_value = stackframes

            process_camera_tick(rc, 'cam1')

        mock_products.run_preview.assert_not_called()
        mock_dbs.set_preview_count.assert_not_called()
        mock_finalize.assert_not_called()


# ---------------------------------------------------------------------------
# Worker loop resilience
# ---------------------------------------------------------------------------

class TestWorkerLoopResilience:

    @patch('banzai.stacking.dbs.cleanup_old_stacks')
    @patch('banzai.stacking.time.sleep')
    @patch('banzai.stacking.process_camera_tick')
    def test_run_worker_loop_continues_after_exception(self, mock_tick, mock_sleep, mock_cleanup):
        """run_worker_loop must not die when process_camera_tick raises; it logs and keeps polling."""
        # First tick raises a normal Exception (caught); second raises KeyboardInterrupt to escape the loop.
        mock_tick.side_effect = [Exception('boom'), KeyboardInterrupt]
        runtime_context_dict = {'db_address': 'sqlite:///fake.db', 'stack_retention_days': 30}
        with pytest.raises(KeyboardInterrupt):
            run_worker_loop('cam1', runtime_context_dict, poll_interval=0)
        assert mock_tick.call_count == 2
        # sleep runs once: after the caught Exception. The KeyboardInterrupt escapes before the second sleep.
        mock_sleep.assert_called_once_with(0)

    @patch('banzai.stacking.dbs.cleanup_old_stacks')
    @patch('banzai.stacking.time.sleep')
    @patch('banzai.stacking.process_camera_tick')
    def test_run_worker_loop_throttles_cleanup(self, mock_tick, mock_sleep, mock_cleanup):
        """cleanup_old_stacks runs on the first tick, then is throttled within the hour window."""
        # Two clean ticks, then KeyboardInterrupt to escape. time.monotonic is left real.
        mock_tick.side_effect = [None, None, KeyboardInterrupt]
        runtime_context_dict = {'db_address': 'sqlite:///fake.db', 'stack_retention_days': 30}
        with pytest.raises(KeyboardInterrupt):
            run_worker_loop('cam1', runtime_context_dict, poll_interval=0)
        assert mock_tick.call_count == 3
        # First tick triggers cleanup; the second is inside the hour window, so it does not.
        mock_cleanup.assert_called_once()


# ---------------------------------------------------------------------------
# Supervisor
# ---------------------------------------------------------------------------

class TestSupervisor:

    @patch('banzai.stacking.sys.exit', side_effect=SystemExit(1))
    @patch('banzai.stacking.multiprocessing.connection.wait')
    @patch('banzai.stacking.multiprocessing.Process')
    @patch('banzai.stacking.dbs.get_instruments_at_site',
           return_value=[SimpleNamespace(camera='cam1'), SimpleNamespace(camera='cam2'),
                         SimpleNamespace(camera='cam3')])
    @patch('banzai.stacking.main.parse_args')
    def test_run_supervisor_spawns_process_per_camera(self, mock_parse, mock_instruments,
                                                      mock_process_cls, mock_wait, mock_exit):
        mock_parse.return_value = SimpleNamespace(
            site_id='tst', db_address='sqlite:///fake.db',
            stack_retention_days=30, stack_timeout_minutes=20, instrument_types='*',
        )
        with pytest.raises(SystemExit):
            run_supervisor()

        assert mock_process_cls.call_count == 3
        assert mock_process_cls.return_value.start.call_count == 3
        mock_wait.assert_called_once()
        mock_exit.assert_called_once_with(1)

    @patch('banzai.stacking.sys.exit', side_effect=SystemExit(1))
    @patch('banzai.stacking.multiprocessing.Process')
    @patch('banzai.stacking.dbs.get_instruments_at_site', return_value=[])
    @patch('banzai.stacking.main.parse_args')
    def test_run_supervisor_exits_when_no_cameras(self, mock_parse, mock_instruments,
                                                  mock_process_cls, mock_exit):
        mock_parse.return_value = SimpleNamespace(
            site_id='tst', db_address='sqlite:///fake.db',
            stack_retention_days=30, stack_timeout_minutes=20, instrument_types='*',
        )
        with pytest.raises(SystemExit):
            run_supervisor()

        mock_process_cls.assert_not_called()
        mock_exit.assert_called_once_with(1)

    @patch('banzai.stacking.sys.exit', side_effect=SystemExit(1))
    @patch('banzai.stacking.multiprocessing.connection.wait')
    @patch('banzai.stacking.multiprocessing.Process')
    @patch('banzai.stacking.dbs.get_instruments_at_site',
           return_value=[SimpleNamespace(camera='cam1', type='1m0-SciCam-Sinistro'),
                         SimpleNamespace(camera='cam2', type='1m0-SciCam-Sinistro'),
                         SimpleNamespace(camera='cam3', type='NRES')])
    @patch('banzai.stacking.main.parse_args')
    def test_run_supervisor_filters_by_instrument_type(self, mock_parse, mock_instruments,
                                                       mock_process_cls, mock_wait, mock_exit):
        mock_parse.return_value = SimpleNamespace(
            site_id='tst', db_address='sqlite:///fake.db',
            stack_retention_days=30, stack_timeout_minutes=20,
            instrument_types='1m0-SciCam-Sinistro',
        )
        with pytest.raises(SystemExit):
            run_supervisor()

        assert mock_process_cls.call_count == 2
