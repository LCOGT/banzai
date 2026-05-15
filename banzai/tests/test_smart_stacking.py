"""Unit tests for the smart stacking feature."""
import datetime
import json
from unittest.mock import MagicMock, patch

import pytest
from astropy.io.fits import Header
from celery.exceptions import Retry

from sqlalchemy import text

from banzai import dbs
from banzai.dbs import insert_subframe, get_subframes, mark_stack_complete, cleanup_old_subframes, update_subframe_filepath
from banzai.stacking import (validate_message, check_stack_complete,
                              push_notification, drain_notifications, REDIS_KEY_PREFIX,
                              process_notifications, finalize_stack, check_timeout,
                              run_worker_loop, StackingSupervisor)
from banzai.scheduling import process_subframe
from banzai.main import SubframeListener

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
        session.add(dbs.Instrument(site='tst', camera='cam1', name='cam1', type='1m0-SciCam-Sinistro', nx=4096, ny=4096))
        session.add(dbs.Instrument(site='tst', camera='cam2', name='cam2', type='1m0-SciCam-Sinistro', nx=4096, ny=4096))
    return addr


@pytest.fixture
def mock_redis():
    """Return a MagicMock standing in for a Redis client."""
    r = MagicMock()
    r.lpush = MagicMock()
    r.lrange = MagicMock(return_value=[])
    r.delete = MagicMock()
    return r


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

class TestDBOperations:

    def test_insert_subframe_and_get_subframes(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        insert_subframe(
            db_address, moluid='mol-001', stack_num=1, frmtotal=5,
            camera='cam1', filepath='/data/frame1.fits', is_last=False, dateobs=dateobs,
        )
        subframes = get_subframes(db_address, moluid='mol-001')
        assert len(subframes) == 1
        subframe = subframes[0]
        assert subframe.moluid == 'mol-001'
        assert subframe.stack_num == 1
        assert subframe.frmtotal == 5
        assert subframe.camera == 'cam1'
        assert subframe.filepath == '/data/frame1.fits'
        assert subframe.is_last is False
        assert subframe.dateobs == dateobs

    def test_insert_subframe_duplicate_resets_state_for_retry(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        insert_subframe(
            db_address, moluid='mol-dup', stack_num=1, frmtotal=3,
            camera='cam1', filepath='/data/dup1.fits', is_last=False, dateobs=dateobs,
        )
        mark_stack_complete(db_address, 'mol-dup', 'complete')
        subframes = get_subframes(db_address, 'mol-dup')
        assert subframes[0].status == 'complete'
        assert subframes[0].completed_at is not None

        new_dateobs = datetime.datetime(2024, 6, 15, 13, 0, 0)
        insert_subframe(
            db_address, moluid='mol-dup', stack_num=1, frmtotal=3,
            camera='cam1', filepath=None, is_last=True, dateobs=new_dateobs,
        )
        subframes = get_subframes(db_address, 'mol-dup')
        assert len(subframes) == 1
        subframe = subframes[0]
        assert subframe.status == 'active'
        assert subframe.completed_at is None
        assert subframe.filepath is None
        assert subframe.is_last is True
        assert subframe.dateobs == new_dateobs

    def test_update_subframe_filepath(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        insert_subframe(
            db_address, moluid='mol-upd', stack_num=1, frmtotal=3,
            camera='cam1', filepath=None, is_last=False, dateobs=dateobs,
        )
        subframes = get_subframes(db_address, 'mol-upd')
        assert subframes[0].filepath is None

        update_subframe_filepath(db_address, 'mol-upd', 1, '/data/reduced.fits')
        subframes = get_subframes(db_address, 'mol-upd')
        assert subframes[0].filepath == '/data/reduced.fits'


# ---------------------------------------------------------------------------
# Status transitions
# ---------------------------------------------------------------------------

class TestStatusTransitions:

    def test_mark_stack_complete_sets_complete(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        for i in range(3):
            insert_subframe(
                db_address, moluid='mol-comp', stack_num=i + 1, frmtotal=3,
                camera='cam1', filepath=f'/data/comp{i}.fits', is_last=(i == 2), dateobs=dateobs,
            )
        mark_stack_complete(db_address, 'mol-comp', 'complete')
        subframes = get_subframes(db_address, 'mol-comp')
        for s in subframes:
            assert s.status == 'complete'
            assert s.completed_at is not None

    def test_mark_stack_complete_sets_timeout(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        for i in range(2):
            insert_subframe(
                db_address, moluid='mol-to', stack_num=i + 1, frmtotal=5,
                camera='cam1', filepath=f'/data/to{i}.fits', is_last=False, dateobs=dateobs,
            )
        mark_stack_complete(db_address, 'mol-to', 'timeout')
        subframes = get_subframes(db_address, 'mol-to')
        for s in subframes:
            assert s.status == 'timeout'
            assert s.completed_at is not None


# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------

class TestTimeout:

    def test_check_timeout_finalizes_stale_stacks(self, db_address):
        old_dateobs = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
        for i in range(3):
            insert_subframe(
                db_address, moluid='mol-stale', stack_num=i + 1, frmtotal=5,
                camera='cam1', filepath=f'/data/stale{i}.fits', is_last=False, dateobs=old_dateobs,
            )
        with dbs.get_session(db_address, site_deploy=True) as session:
            session.execute(
                text("UPDATE subframes SET created_at = :old WHERE moluid = :mol"),
                {'old': datetime.datetime.utcnow() - datetime.timedelta(hours=2), 'mol': 'mol-stale'},
            )
        check_timeout(db_address, 'cam1', timeout_minutes=60)
        subframes = get_subframes(db_address, 'mol-stale')
        for s in subframes:
            assert s.status == 'timeout'

    def test_check_timeout_applies_to_null_dateobs_rows(self, db_address):
        """Rows with NULL dateobs ingested before the cutoff must be timed out.

        The old dateobs-based filter silently excluded these rows because SQL
        NULL comparisons evaluate to UNKNOWN (never TRUE).
        """
        insert_subframe(
            db_address, moluid='mol-null-date', stack_num=1, frmtotal=3,
            camera='cam1', filepath='/data/null1.fits', is_last=False, dateobs=None,
        )
        # Back-date created_at so the row looks old enough to be stale.
        with dbs.get_session(db_address, site_deploy=True) as session:
            session.execute(
                text("UPDATE subframes SET created_at = :old WHERE moluid = :mol"),
                {'old': datetime.datetime.utcnow() - datetime.timedelta(hours=2), 'mol': 'mol-null-date'},
            )
        check_timeout(db_address, 'cam1', timeout_minutes=60)
        subframes = get_subframes(db_address, 'mol-null-date')
        assert len(subframes) == 1
        assert subframes[0].status == 'timeout', (
            "NULL-dateobs row was not timed out; filter must use created_at, not dateobs"
        )

    def test_check_timeout_spares_recently_created_row_with_old_dateobs(self, db_address):
        """A row with an ancient dateobs but a recent created_at must not be timed out.

        This covers the reprocessing case: old data is re-ingested now, so
        created_at is recent even though dateobs is in the distant past.
        """
        ancient_dateobs = datetime.datetime(2020, 1, 1, 0, 0, 0)
        insert_subframe(
            db_address, moluid='mol-reprocess', stack_num=1, frmtotal=3,
            camera='cam1', filepath='/data/reproc1.fits', is_last=False, dateobs=ancient_dateobs,
        )
        # created_at defaults to utcnow() — well within the timeout window.
        check_timeout(db_address, 'cam1', timeout_minutes=60)
        subframes = get_subframes(db_address, 'mol-reprocess')
        assert len(subframes) == 1
        assert subframes[0].status == 'active', (
            "Reprocessed row with recent created_at was incorrectly timed out"
        )


# ---------------------------------------------------------------------------
# Redis notifications
# ---------------------------------------------------------------------------

class TestRedisNotifications:

    def test_push_notification(self, mock_redis):
        push_notification(mock_redis, 'cam1', 'mol-abc')
        mock_redis.lpush.assert_called_once_with(f'{REDIS_KEY_PREFIX}cam1', 'mol-abc')

    def test_drain_notifications_for_camera(self, mock_redis):
        mock_redis.lrange.return_value = [b'mol-a', b'mol-a', b'mol-b']
        result = drain_notifications(mock_redis, 'cam1')
        assert result == {'mol-a', 'mol-b'}
        mock_redis.rename.assert_called_once_with(
            f'{REDIS_KEY_PREFIX}cam1', f'{REDIS_KEY_PREFIX}cam1:draining')
        mock_redis.lrange.assert_called_once_with(f'{REDIS_KEY_PREFIX}cam1:draining', 0, -1)
        mock_redis.delete.assert_called_once_with(f'{REDIS_KEY_PREFIX}cam1:draining')


# ---------------------------------------------------------------------------
# Multiple concurrent stacks
# ---------------------------------------------------------------------------

class TestConcurrentStacks:

    def test_check_stack_complete_handles_concurrent_stacks_same_camera(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        for i in range(3):
            insert_subframe(
                db_address, moluid='mol-A', stack_num=i + 1, frmtotal=3,
                camera='cam1', filepath=f'/data/a{i}.fits', is_last=(i == 2), dateobs=dateobs,
            )
        for i in range(2):
            insert_subframe(
                db_address, moluid='mol-B', stack_num=i + 1, frmtotal=5,
                camera='cam1', filepath=f'/data/b{i}.fits', is_last=False, dateobs=dateobs,
            )

        subframes_a = get_subframes(db_address, 'mol-A')
        subframes_b = get_subframes(db_address, 'mol-B')
        assert len(subframes_a) == 3
        assert len(subframes_b) == 2
        assert check_stack_complete(subframes_a, frmtotal=3) is True
        assert check_stack_complete(subframes_b, frmtotal=5) is False


# ---------------------------------------------------------------------------
# check_stack_complete
# ---------------------------------------------------------------------------

class TestCheckStackComplete:

    @staticmethod
    def _subframe(filepath='/data/f.fits', is_last=False):
        f = MagicMock()
        f.filepath = filepath
        f.is_last = is_last
        return f

    def test_check_stack_complete_all_subframes_arrived_and_reduced(self):
        subframes = [self._subframe() for _ in range(3)]
        assert check_stack_complete(subframes, frmtotal=3) is True

    def test_check_stack_complete_partial_without_is_last(self):
        subframes = [self._subframe() for _ in range(3)]
        assert check_stack_complete(subframes, frmtotal=5) is False

    def test_check_stack_complete_partial_with_is_last(self):
        subframes = [self._subframe() for _ in range(2)] + [self._subframe(is_last=True)]
        assert check_stack_complete(subframes, frmtotal=5) is True

    def test_check_stack_complete_is_last_waits_for_unreduced_subframes(self):
        subframes = [self._subframe(), self._subframe(filepath=None, is_last=True)]
        assert check_stack_complete(subframes, frmtotal=5) is False

    def test_check_stack_complete_empty_subframes(self):
        assert check_stack_complete([], frmtotal=5) is False


# ---------------------------------------------------------------------------
# Retention / cleanup
# ---------------------------------------------------------------------------

class TestRetention:

    def test_cleanup_old_subframes(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        for i in range(3):
            insert_subframe(
                db_address, moluid='mol-old', stack_num=i + 1, frmtotal=3,
                camera='cam1', filepath=f'/data/old{i}.fits', is_last=(i == 2), dateobs=dateobs,
            )
        mark_stack_complete(db_address, 'mol-old', 'complete')

        with dbs.get_session(db_address) as session:
            session.execute(
                text("UPDATE subframes SET completed_at = :old_date WHERE moluid = :mol"),
                {'old_date': datetime.datetime.utcnow() - datetime.timedelta(days=30), 'mol': 'mol-old'},
            )

        cleanup_old_subframes(db_address, retention_days=7)
        subframes = get_subframes(db_address, 'mol-old')
        assert len(subframes) == 0

    def test_cleanup_old_subframes_preserves_recent(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        for i in range(3):
            insert_subframe(
                db_address, moluid='mol-recent', stack_num=i + 1, frmtotal=3,
                camera='cam1', filepath=f'/data/recent{i}.fits', is_last=(i == 2), dateobs=dateobs,
            )
        mark_stack_complete(db_address, 'mol-recent', 'complete')
        cleanup_old_subframes(db_address, retention_days=7)
        subframes = get_subframes(db_address, 'mol-recent')
        assert len(subframes) == 3


# ---------------------------------------------------------------------------
# SubframeListener on_message
# ---------------------------------------------------------------------------

class TestSubframeListenerOnMessage:
    """on_message dispatches to Celery; no FITS I/O or DB work here."""

    @patch('banzai.main.process_subframe')
    def test_on_message_dispatches_valid(self, mock_task):
        ctx = MagicMock(SUBFRAME_TASK_QUEUE_NAME='subframe_tasks')
        listener = SubframeListener(ctx)

        body = {
            'fits_file': '/path/to/frame.fits',
            'last_frame': False,
            'instrument_enqueue_timestamp': 1771023918500,
        }
        mock_message = MagicMock()

        listener.on_message(body, mock_message)

        mock_task.apply_async.assert_called_once_with(
            args=(body, vars(ctx)),
            queue='subframe_tasks',
        )
        mock_message.ack.assert_called_once()

    @patch('banzai.main.process_subframe')
    def test_on_message_parses_json_string(self, mock_task):
        ctx = MagicMock(SUBFRAME_TASK_QUEUE_NAME='subframe_tasks')
        listener = SubframeListener(ctx)

        body = {
            'fits_file': '/path/to/frame.fits',
            'last_frame': False,
            'instrument_enqueue_timestamp': 1771023918500,
        }
        mock_message = MagicMock()

        listener.on_message(json.dumps(body), mock_message)

        mock_task.apply_async.assert_called_once_with(
            args=(body, vars(ctx)),
            queue='subframe_tasks',
        )
        mock_message.ack.assert_called_once()

    @patch('banzai.main.process_subframe')
    def test_on_message_parses_json_bytes(self, mock_task):
        ctx = MagicMock(SUBFRAME_TASK_QUEUE_NAME='subframe_tasks')
        listener = SubframeListener(ctx)

        body = {
            'fits_file': '/path/to/frame.fits',
            'last_frame': False,
            'instrument_enqueue_timestamp': 1771023918500,
        }
        mock_message = MagicMock()

        listener.on_message(json.dumps(body).encode('utf-8'), mock_message)

        mock_task.apply_async.assert_called_once_with(
            args=(body, vars(ctx)),
            queue='subframe_tasks',
        )
        mock_message.ack.assert_called_once()

    @patch('banzai.main.process_subframe')
    def test_on_message_invalid_no_dispatch(self, mock_task):
        listener = SubframeListener(MagicMock())

        body = {
            'last_frame': True,
            # missing fits_file and instrument_enqueue_timestamp
        }
        mock_message = MagicMock()

        listener.on_message(body, mock_message)

        mock_task.apply_async.assert_not_called()
        mock_message.ack.assert_called_once()

    @patch('banzai.main.process_subframe')
    def test_on_message_malformed_json_acks_and_no_dispatch(self, mock_task):
        ctx = MagicMock(SUBFRAME_TASK_QUEUE_NAME='subframe_tasks')
        listener = SubframeListener(ctx)
        mock_message = MagicMock()

        listener.on_message('{not valid json}', mock_message)

        mock_task.apply_async.assert_not_called()
        mock_message.ack.assert_called_once()

    @patch('banzai.main.process_subframe')
    def test_on_message_invalid_utf8_acks_and_no_dispatch(self, mock_task):
        ctx = MagicMock(SUBFRAME_TASK_QUEUE_NAME='subframe_tasks')
        listener = SubframeListener(ctx)
        mock_message = MagicMock()

        listener.on_message(b'\xff', mock_message)

        mock_task.apply_async.assert_not_called()
        mock_message.ack.assert_called_once()

    @patch('banzai.main.process_subframe')
    def test_on_message_non_object_json_acks_and_no_dispatch(self, mock_task):
        ctx = MagicMock(SUBFRAME_TASK_QUEUE_NAME='subframe_tasks')
        listener = SubframeListener(ctx)
        mock_message = MagicMock()

        listener.on_message(json.dumps(['not', 'an', 'object']), mock_message)

        mock_task.apply_async.assert_not_called()
        mock_message.ack.assert_called_once()


# ---------------------------------------------------------------------------
# process_subframe Celery task
# ---------------------------------------------------------------------------

class TestProcessSubframe:
    """Test the Celery task that does the actual subframe processing."""

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
    def test_process_subframe(self, mock_run_stages, last_frame_val, expected_is_last, db_address, mock_redis):

        mock_image = self._make_mock_image()
        mock_run_stages.return_value = [mock_image]

        header = self._make_fits_header()
        body = {
            'fits_file': '/path/to/frame.fits',
            'last_frame': last_frame_val,
            'instrument_enqueue_timestamp': 1771023918500,
        }
        runtime_context = {'db_address': db_address, 'REDIS_URL': 'redis://localhost:6379/0'}

        with patch('banzai.scheduling.fits_utils.get_primary_header', return_value=header), \
             patch('banzai.scheduling.redis.Redis.from_url', return_value=mock_redis):
            process_subframe(body, runtime_context)

        mock_run_stages.assert_called_once()

        subframes = get_subframes(db_address, 'mol-xyz')
        assert len(subframes) == 1
        assert subframes[0].stack_num == 1
        assert subframes[0].frmtotal == 5
        assert subframes[0].camera == 'cam1'
        assert subframes[0].is_last is expected_is_last
        assert subframes[0].filepath == '/data/processed/frame-e09.fits'
        mock_redis.lpush.assert_called_once()

    def test_process_subframe_retries_on_unreadable_header(self, db_address):
        """If get_primary_header returns None (I/O error), the task must retry, not swallow the failure."""
        body = {
            'fits_file': '/path/to/corrupt.fits',
            'last_frame': True,
            'instrument_enqueue_timestamp': 1771023918500,
        }
        runtime_context = {'db_address': db_address, 'REDIS_URL': 'redis://localhost:6379/0'}

        with patch('banzai.scheduling.fits_utils.get_primary_header', return_value=None), \
             patch.object(process_subframe, 'retry', side_effect=Retry()) as mock_retry:
            with pytest.raises(Retry):
                process_subframe(body, runtime_context)

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
