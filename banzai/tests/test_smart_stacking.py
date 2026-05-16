"""Unit tests for the smart stacking feature."""
import datetime
import json
from unittest.mock import MagicMock, patch

import pytest
from astropy.io.fits import Header
from celery.exceptions import Retry

from sqlalchemy import text

from banzai import dbs
from banzai.dbs import insert_subframe, get_subframes, mark_stack_complete, cleanup_old_subframes
from banzai.stacking import (validate_message, check_stack_complete,
                              push_notification, drain_notifications, REDIS_KEY_PREFIX,
                              process_notifications, finalize_stack, check_timeouts,
                              stack_has_timed_out, adaptive_timeout_baseline_seconds,
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
        assert subframe.exptime is None

    def test_insert_subframe_stores_exptime(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        insert_subframe(
            db_address, moluid='mol-exp', stack_num=1, frmtotal=5,
            camera='cam1', filepath='/data/frame1.fits', is_last=False, dateobs=dateobs,
            exptime=30.0,
        )
        subframes = get_subframes(db_address, moluid='mol-exp')
        assert len(subframes) == 1
        assert subframes[0].exptime == 30.0

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
            camera='cam1', filepath='/data/dup2.fits', is_last=True, dateobs=new_dateobs,
        )
        subframes = get_subframes(db_address, 'mol-dup')
        assert len(subframes) == 1
        subframe = subframes[0]
        assert subframe.status == 'active'
        assert subframe.completed_at is None
        assert subframe.filepath == '/data/dup2.fits'
        assert subframe.is_last is True
        assert subframe.dateobs == new_dateobs

    def test_insert_subframe_duplicate_refreshes_exptime(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        insert_subframe(
            db_address, moluid='mol-exp-dup', stack_num=1, frmtotal=3,
            camera='cam1', filepath='/data/dup1.fits', is_last=False, dateobs=dateobs,
            exptime=10.0,
        )
        insert_subframe(
            db_address, moluid='mol-exp-dup', stack_num=1, frmtotal=3,
            camera='cam1', filepath='/data/dup2.fits', is_last=False, dateobs=dateobs,
            exptime=45.0,
        )
        subframes = get_subframes(db_address, 'mol-exp-dup')
        assert len(subframes) == 1
        assert subframes[0].exptime == 45.0
        assert subframes[0].filepath == '/data/dup2.fits'

    def test_insert_subframe_requires_filepath(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        with pytest.raises(ValueError, match='filepath is required'):
            insert_subframe(
                db_address, moluid='mol-upd', stack_num=1, frmtotal=3,
                camera='cam1', filepath=None, is_last=False, dateobs=dateobs,
            )


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
# Adaptive timeout
# ---------------------------------------------------------------------------

class TestAdaptiveTimeout:

    @staticmethod
    def _created(base, seconds):
        return base + datetime.timedelta(seconds=seconds)

    @staticmethod
    def _insert_frame(db_address, moluid, stack_num, created_at, camera='cam1',
                      filepath='/data/reduced.fits', frmtotal=5, exptime=30.0, is_last=False):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        insert_subframe(
            db_address, moluid=moluid, stack_num=stack_num, frmtotal=frmtotal,
            camera=camera, filepath=filepath, is_last=is_last, dateobs=dateobs,
            exptime=exptime,
        )
        with dbs.get_session(db_address, site_deploy=True) as session:
            session.execute(
                text("UPDATE subframes SET created_at = :created_at WHERE moluid = :moluid AND stack_num = :stack_num"),
                {'created_at': created_at, 'moluid': moluid, 'stack_num': stack_num},
            )

    def test_one_frame_stack_uses_exptime_plus_initial_buffer(self, db_address):
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        self._insert_frame(db_address, 'mol-one', 1, start, exptime=30.0)

        subframes = get_subframes(db_address, 'mol-one')
        assert stack_has_timed_out(subframes, now=self._created(start, 90)) is False
        assert stack_has_timed_out(subframes, now=self._created(start, 91)) is True

    def test_two_frame_stack_uses_first_to_second_baseline(self, db_address):
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        self._insert_frame(db_address, 'mol-two', 1, start, exptime=30.0)
        self._insert_frame(db_address, 'mol-two', 2, self._created(start, 120), exptime=30.0)

        subframes = get_subframes(db_address, 'mol-two')
        assert adaptive_timeout_baseline_seconds(subframes) == 90.0
        assert stack_has_timed_out(subframes, now=self._created(start, 330)) is False
        assert stack_has_timed_out(subframes, now=self._created(start, 331)) is True

    def test_positive_baseline_below_fallback_is_used_directly(self, db_address):
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        self._insert_frame(db_address, 'mol-fast', 1, start, exptime=30.0)
        self._insert_frame(db_address, 'mol-fast', 2, self._created(start, 40), exptime=30.0)

        subframes = get_subframes(db_address, 'mol-fast')
        assert adaptive_timeout_baseline_seconds(subframes) == 10.0
        assert stack_has_timed_out(subframes, now=self._created(start, 89)) is False
        assert stack_has_timed_out(subframes, now=self._created(start, 91)) is True

    def test_baseline_uses_fallback_when_adjusted_gap_is_nonpositive(self, db_address):
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        self._insert_frame(db_address, 'mol-clamp', 1, start, exptime=30.0)
        self._insert_frame(db_address, 'mol-clamp', 2, self._created(start, 20), exptime=30.0)

        subframes = get_subframes(db_address, 'mol-clamp')
        assert adaptive_timeout_baseline_seconds(subframes) == 60.0
        assert stack_has_timed_out(subframes, now=self._created(start, 170)) is False
        assert stack_has_timed_out(subframes, now=self._created(start, 171)) is True

    def test_late_observed_later_frame_gap_triggers_timeout(self, db_address):
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        self._insert_frame(db_address, 'mol-late-gap', 1, start, exptime=30.0)
        self._insert_frame(db_address, 'mol-late-gap', 2, self._created(start, 120), exptime=30.0)
        self._insert_frame(db_address, 'mol-late-gap', 3, self._created(start, 331), exptime=30.0)

        subframes = get_subframes(db_address, 'mol-late-gap')
        assert stack_has_timed_out(subframes, now=self._created(start, 340)) is True

    def test_missing_next_frame_beyond_threshold_triggers_timeout(self, db_address):
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        self._insert_frame(db_address, 'mol-missing', 1, start, exptime=30.0)
        self._insert_frame(db_address, 'mol-missing', 2, self._created(start, 120), exptime=30.0)

        subframes = get_subframes(db_address, 'mol-missing')
        assert stack_has_timed_out(subframes, now=self._created(start, 331)) is True

    def test_incomplete_stack_inside_timeout_window_remains_active(self, db_address):
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        self._insert_frame(db_address, 'mol-active', 1, start, exptime=30.0)
        self._insert_frame(db_address, 'mol-active', 2, self._created(start, 120), exptime=30.0)

        check_timeouts(db_address, 'cam1', now=self._created(start, 200))

        subframes = get_subframes(db_address, 'mol-active')
        assert {s.status for s in subframes} == {'active'}

    def test_check_timeouts_marks_timeout_for_worker_camera_only(self, db_address):
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        self._insert_frame(db_address, 'mol-timeout', 1, start, camera='cam1', exptime=30.0)
        self._insert_frame(db_address, 'mol-other-camera', 1, start, camera='cam2', exptime=30.0)

        check_timeouts(db_address, 'cam1', now=self._created(start, 91))

        assert {s.status for s in get_subframes(db_address, 'mol-timeout')} == {'timeout'}
        assert {s.status for s in get_subframes(db_address, 'mol-other-camera')} == {'active'}

    def test_check_timeouts_marks_complete_before_timeout(self, db_address):
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        for stack_num in range(1, 4):
            self._insert_frame(
                db_address, 'mol-complete-wins', stack_num, self._created(start, stack_num * 300),
                frmtotal=3, exptime=30.0, is_last=(stack_num == 3),
            )

        check_timeouts(db_address, 'cam1', now=self._created(start, 1000))

        assert {s.status for s in get_subframes(db_address, 'mol-complete-wins')} == {'complete'}

    def test_check_timeouts_ignores_terminal_rows(self, db_address):
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
        self._insert_frame(db_address, 'mol-terminal', 1, start, exptime=30.0)
        mark_stack_complete(db_address, 'mol-terminal', 'complete')

        check_timeouts(db_address, 'cam1', now=self._created(start, 91))

        assert {s.status for s in get_subframes(db_address, 'mol-terminal')} == {'complete'}


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

    def test_check_stack_complete_all_subframes_arrived(self):
        subframes = [self._subframe() for _ in range(3)]
        assert check_stack_complete(subframes, frmtotal=3) is True

    def test_check_stack_complete_partial_without_is_last(self):
        subframes = [self._subframe() for _ in range(3)]
        assert check_stack_complete(subframes, frmtotal=5) is False

    def test_check_stack_complete_partial_with_is_last(self):
        subframes = [self._subframe() for _ in range(2)] + [self._subframe(is_last=True)]
        assert check_stack_complete(subframes, frmtotal=5) is True

    def test_check_stack_complete_empty_subframes(self):
        assert check_stack_complete([], frmtotal=5) is False

    def test_check_stack_complete_empty_subframes_with_zero_total(self):
        assert check_stack_complete([], frmtotal=0) is False


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
        h['EXPTIME'] = 30.0
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
        assert subframes[0].exptime == 30.0
        assert subframes[0].filepath == '/data/processed/frame-e09.fits'
        mock_redis.lpush.assert_called_once()

    @patch('banzai.scheduling.insert_subframe')
    @patch('banzai.scheduling.stage_utils.run_pipeline_stages')
    def test_process_subframe_inserts_only_after_reduction(self, mock_run_stages, mock_insert, db_address,
                                                           mock_redis):
        mock_image = self._make_mock_image()

        def _run_stages(*args, **kwargs):
            mock_insert.assert_not_called()
            return [mock_image]

        mock_run_stages.side_effect = _run_stages
        header = self._make_fits_header()
        body = {
            'fits_file': '/path/to/frame.fits',
            'last_frame': False,
            'instrument_enqueue_timestamp': 1771023918500,
        }
        runtime_context = {'db_address': db_address, 'REDIS_URL': 'redis://localhost:6379/0'}

        with patch('banzai.scheduling.fits_utils.get_primary_header', return_value=header), \
             patch('banzai.scheduling.redis.Redis.from_url', return_value=mock_redis):
            process_subframe(body, runtime_context)

        mock_insert.assert_called_once()
        assert mock_insert.call_args.kwargs['filepath'] == '/data/processed/frame-e09.fits'
        mock_redis.lpush.assert_called_once()

    @patch('banzai.scheduling.stage_utils.run_pipeline_stages')
    def test_process_subframe_does_not_insert_or_notify_without_reduced_image(self, mock_run_stages, db_address,
                                                                              mock_redis):
        mock_run_stages.return_value = []
        header = self._make_fits_header()
        body = {
            'fits_file': '/path/to/frame.fits',
            'last_frame': False,
            'instrument_enqueue_timestamp': 1771023918500,
        }
        runtime_context = {'db_address': db_address, 'REDIS_URL': 'redis://localhost:6379/0'}

        with patch('banzai.scheduling.fits_utils.get_primary_header', return_value=header), \
             patch('banzai.scheduling.redis.Redis.from_url', return_value=mock_redis):
            process_subframe(body, runtime_context)

        assert get_subframes(db_address, 'mol-xyz') == []
        mock_redis.lpush.assert_not_called()

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
    @patch('banzai.stacking.check_timeouts')
    @patch('banzai.stacking.dbs.cleanup_old_subframes')
    @patch('banzai.stacking.process_notifications')
    def test_run_worker_loop_continues_after_exception(
        self, mock_process, mock_cleanup, mock_check_timeouts, mock_sleep, mock_redis_cls
    ):
        """run_worker_loop must not crash when process_notifications raises; it should log and continue."""
        # First call raises, second call raises KeyboardInterrupt to escape the infinite loop.
        mock_process.side_effect = [Exception('boom'), KeyboardInterrupt]
        with pytest.raises(KeyboardInterrupt):
            run_worker_loop('cam1', 'sqlite:///fake.db', 'redis://localhost:6379', poll_interval=0)
        # process_notifications was called twice: once raised Exception, once raised KeyboardInterrupt.
        assert mock_process.call_count == 2
        mock_check_timeouts.assert_not_called()
        mock_cleanup.assert_not_called()
        # sleep should have been called once (after the transient Exception, before continuing).
        mock_sleep.assert_called_once_with(0)

    @patch('banzai.stacking.redis_lib.Redis.from_url')
    @patch('banzai.stacking.time.sleep')
    @patch('banzai.stacking.check_timeouts')
    @patch('banzai.stacking.dbs.cleanup_old_subframes')
    @patch('banzai.stacking.process_notifications')
    def test_run_worker_loop_invokes_timeout_sweep(
        self, mock_process, mock_cleanup, mock_check_timeouts, mock_sleep, mock_redis_cls
    ):
        """run_worker_loop should sweep adaptive timeouts after notification processing."""
        mock_process.side_effect = [None, KeyboardInterrupt]
        with pytest.raises(KeyboardInterrupt):
            run_worker_loop('cam1', 'sqlite:///fake.db', 'redis://localhost:6379', poll_interval=0)

        mock_check_timeouts.assert_called_once_with('sqlite:///fake.db', 'cam1')
        mock_cleanup.assert_called_once_with('sqlite:///fake.db', 30)
        mock_sleep.assert_called_once_with(0)
