"""Unit tests for the smart stacking feature."""
import datetime
from unittest.mock import MagicMock, patch

import pytest
from astropy.io.fits import Header

from sqlalchemy import text

from banzai import dbs
from banzai.dbs import insert_stack_frame, get_stack_frames, mark_stack_complete, cleanup_old_records, update_stack_frame_filepath
from banzai.stacking import (validate_message, check_stack_complete,
                              push_notification, drain_notifications, REDIS_KEY_PREFIX,
                              process_notifications, finalize_stack, check_timeout,
                              discover_cameras, StackingSupervisor)
from banzai.main import SubframeListener

pytestmark = pytest.mark.smart_stacking


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db_address(tmp_path):
    """Create a fresh SQLite DB per test with a site and two instruments."""
    addr = f'sqlite:///{tmp_path}/test.db'
    dbs.create_db(addr)
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

@pytest.mark.smart_stacking
class TestDBOperations:

    def test_insert_and_query(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        insert_stack_frame(
            db_address, moluid='mol-001', stack_num=1, frmtotal=5,
            camera='cam1', filepath='/data/frame1.fits', is_last=False, dateobs=dateobs,
        )
        frames = get_stack_frames(db_address, moluid='mol-001')
        assert len(frames) == 1
        frame = frames[0]
        assert frame.moluid == 'mol-001'
        assert frame.stack_num == 1
        assert frame.frmtotal == 5
        assert frame.camera == 'cam1'
        assert frame.filepath == '/data/frame1.fits'
        assert frame.is_last is False
        assert frame.dateobs == dateobs

    def test_duplicate_is_noop(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        insert_stack_frame(
            db_address, moluid='mol-dup', stack_num=1, frmtotal=3,
            camera='cam1', filepath='/data/dup1.fits', is_last=False, dateobs=dateobs,
        )
        insert_stack_frame(
            db_address, moluid='mol-dup', stack_num=1, frmtotal=3,
            camera='cam1', filepath='/data/dup2.fits', is_last=False, dateobs=dateobs,
        )
        frames = get_stack_frames(db_address, 'mol-dup')
        assert len(frames) == 1
        assert frames[0].filepath == '/data/dup1.fits'

    def test_update_stack_frame_filepath(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        insert_stack_frame(
            db_address, moluid='mol-upd', stack_num=1, frmtotal=3,
            camera='cam1', filepath=None, is_last=False, dateobs=dateobs,
        )
        frames = get_stack_frames(db_address, 'mol-upd')
        assert frames[0].filepath is None

        update_stack_frame_filepath(db_address, 'mol-upd', 1, '/data/reduced.fits')
        frames = get_stack_frames(db_address, 'mol-upd')
        assert frames[0].filepath == '/data/reduced.fits'


# ---------------------------------------------------------------------------
# Status transitions
# ---------------------------------------------------------------------------

@pytest.mark.smart_stacking
class TestStatusTransitions:

    def test_status_active_to_complete(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        for i in range(3):
            insert_stack_frame(
                db_address, moluid='mol-comp', stack_num=i + 1, frmtotal=3,
                camera='cam1', filepath=f'/data/comp{i}.fits', is_last=(i == 2), dateobs=dateobs,
            )
        mark_stack_complete(db_address, 'mol-comp', 'complete')
        frames = get_stack_frames(db_address, 'mol-comp')
        for f in frames:
            assert f.status == 'complete'
            assert f.completed_at is not None

    def test_status_active_to_timeout(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        for i in range(2):
            insert_stack_frame(
                db_address, moluid='mol-to', stack_num=i + 1, frmtotal=5,
                camera='cam1', filepath=f'/data/to{i}.fits', is_last=False, dateobs=dateobs,
            )
        mark_stack_complete(db_address, 'mol-to', 'timeout')
        frames = get_stack_frames(db_address, 'mol-to')
        for f in frames:
            assert f.status == 'timeout'
            assert f.completed_at is not None


# ---------------------------------------------------------------------------
# Timeout
# ---------------------------------------------------------------------------

@pytest.mark.smart_stacking
class TestTimeout:

    def test_timeout_finalizes_stale_stacks(self, db_address):
        old_dateobs = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
        for i in range(3):
            insert_stack_frame(
                db_address, moluid='mol-stale', stack_num=i + 1, frmtotal=5,
                camera='cam1', filepath=f'/data/stale{i}.fits', is_last=False, dateobs=old_dateobs,
            )
        check_timeout(db_address, 'cam1', timeout_minutes=60)
        frames = get_stack_frames(db_address, 'mol-stale')
        for f in frames:
            assert f.status == 'timeout'


# ---------------------------------------------------------------------------
# Redis notifications
# ---------------------------------------------------------------------------

@pytest.mark.smart_stacking
class TestRedisNotifications:

    def test_push_notification(self, mock_redis):
        push_notification(mock_redis, 'cam1', 'mol-abc')
        mock_redis.lpush.assert_called_once_with(f'{REDIS_KEY_PREFIX}cam1', 'mol-abc')

    def test_drain_for_camera(self, mock_redis):
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

@pytest.mark.smart_stacking
class TestConcurrentStacks:

    def test_concurrent_stacks_same_camera(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        for i in range(3):
            insert_stack_frame(
                db_address, moluid='mol-A', stack_num=i + 1, frmtotal=3,
                camera='cam1', filepath=f'/data/a{i}.fits', is_last=(i == 2), dateobs=dateobs,
            )
        for i in range(2):
            insert_stack_frame(
                db_address, moluid='mol-B', stack_num=i + 1, frmtotal=5,
                camera='cam1', filepath=f'/data/b{i}.fits', is_last=False, dateobs=dateobs,
            )

        frames_a = get_stack_frames(db_address, 'mol-A')
        frames_b = get_stack_frames(db_address, 'mol-B')
        assert len(frames_a) == 3
        assert len(frames_b) == 2
        assert check_stack_complete(frames_a, frmtotal=3) is True
        assert check_stack_complete(frames_b, frmtotal=5) is False


# ---------------------------------------------------------------------------
# check_stack_complete
# ---------------------------------------------------------------------------

@pytest.mark.smart_stacking
class TestCheckStackComplete:

    @staticmethod
    def _frame(filepath='/data/f.fits', is_last=False):
        f = MagicMock()
        f.filepath = filepath
        f.is_last = is_last
        return f

    def test_all_frames_arrived_and_reduced(self):
        frames = [self._frame() for _ in range(3)]
        assert check_stack_complete(frames, frmtotal=3) is True

    def test_partial_without_is_last(self):
        frames = [self._frame() for _ in range(3)]
        assert check_stack_complete(frames, frmtotal=5) is False

    def test_partial_with_is_last(self):
        frames = [self._frame() for _ in range(2)] + [self._frame(is_last=True)]
        assert check_stack_complete(frames, frmtotal=5) is True

    def test_is_last_waits_for_unreduced_frames(self):
        frames = [self._frame(), self._frame(filepath=None, is_last=True)]
        assert check_stack_complete(frames, frmtotal=5) is False

    def test_empty_frames(self):
        assert check_stack_complete([], frmtotal=5) is False


# ---------------------------------------------------------------------------
# Retention / cleanup
# ---------------------------------------------------------------------------

@pytest.mark.smart_stacking
class TestRetention:

    def test_cleanup_old_records(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        for i in range(3):
            insert_stack_frame(
                db_address, moluid='mol-old', stack_num=i + 1, frmtotal=3,
                camera='cam1', filepath=f'/data/old{i}.fits', is_last=(i == 2), dateobs=dateobs,
            )
        mark_stack_complete(db_address, 'mol-old', 'complete')

        with dbs.get_session(db_address) as session:
            session.execute(
                text("UPDATE stack_frames SET completed_at = :old_date WHERE moluid = :mol"),
                {'old_date': datetime.datetime.utcnow() - datetime.timedelta(days=30), 'mol': 'mol-old'},
            )

        cleanup_old_records(db_address, retention_days=7)
        frames = get_stack_frames(db_address, 'mol-old')
        assert len(frames) == 0

    def test_cleanup_preserves_recent(self, db_address):
        dateobs = datetime.datetime(2024, 6, 15, 12, 0, 0)
        for i in range(3):
            insert_stack_frame(
                db_address, moluid='mol-recent', stack_num=i + 1, frmtotal=3,
                camera='cam1', filepath=f'/data/recent{i}.fits', is_last=(i == 2), dateobs=dateobs,
            )
        mark_stack_complete(db_address, 'mol-recent', 'complete')
        cleanup_old_records(db_address, retention_days=7)
        frames = get_stack_frames(db_address, 'mol-recent')
        assert len(frames) == 3


# ---------------------------------------------------------------------------
# SubframeListener on_message
# ---------------------------------------------------------------------------

@pytest.mark.smart_stacking
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


# ---------------------------------------------------------------------------
# process_subframe Celery task
# ---------------------------------------------------------------------------

@pytest.mark.smart_stacking
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
        from banzai.scheduling import process_subframe

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

        frames = get_stack_frames(db_address, 'mol-xyz')
        assert len(frames) == 1
        assert frames[0].stack_num == 1
        assert frames[0].frmtotal == 5
        assert frames[0].camera == 'cam1'
        assert frames[0].is_last is expected_is_last
        assert frames[0].filepath == '/data/processed/frame-e09.fits'
        mock_redis.lpush.assert_called_once()


# ---------------------------------------------------------------------------
# Supervisor
# ---------------------------------------------------------------------------

@pytest.mark.smart_stacking
class TestSupervisor:

    def test_discover_cameras(self, db_address):
        cameras = discover_cameras(db_address, 'tst')
        assert 'cam1' in cameras
        assert 'cam2' in cameras
        assert len(cameras) == 2

    @patch('banzai.stacking.discover_cameras', return_value=['cam1', 'cam2', 'cam3'])
    @patch('banzai.stacking.multiprocessing.Process')
    def test_supervisor_spawns_per_camera(self, mock_process_cls, mock_discover):
        supervisor = StackingSupervisor(
            site_id='tst',
            db_address='sqlite:///fake.db',
            redis_url='redis://localhost:6379',
        )
        supervisor.start()
        assert mock_process_cls.call_count == 3
        assert mock_process_cls.return_value.start.call_count == 3
