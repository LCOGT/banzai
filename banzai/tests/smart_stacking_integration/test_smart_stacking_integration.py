"""
Integration tests for smart stacking against real PostgreSQL + Redis.

These tests call stacking functions directly -- no banzai containers run.
"""

import datetime

import pytest

from banzai.dbs import insert_stack_frame, get_stack_frames
from banzai.stacking import check_timeout, process_notifications, push_notification


@pytest.mark.integration_smart_stacking
class TestSmartStackingIntegration:

    def test_timeout_flow(self, infrastructure, db_address):
        """Insert incomplete stack with old timestamps, verify timeout."""
        old_time = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
        for i in range(1, 3):  # 2 of 5
            insert_stack_frame(db_address, 'mol-timeout', i, 5, 'cam1',
                               f'/tmp/t{i}.fits', False, old_time)

        check_timeout(db_address, 'cam1', timeout_minutes=1)

        frames = get_stack_frames(db_address, 'mol-timeout')
        assert all(f.status == 'timeout' for f in frames)

    def test_drain_and_process_latest(self, infrastructure, db_address, redis_client):
        """Push multiple notifications, insert frames between, verify latest state used."""
        push_notification(redis_client, 'cam1', 'mol-drain')

        now = datetime.datetime.utcnow()
        insert_stack_frame(db_address, 'mol-drain', 1, 3, 'cam1', '/tmp/d1.fits', False, now)
        insert_stack_frame(db_address, 'mol-drain', 2, 3, 'cam1', '/tmp/d2.fits', False, now)

        push_notification(redis_client, 'cam1', 'mol-drain')

        insert_stack_frame(db_address, 'mol-drain', 3, 3, 'cam1', '/tmp/d3.fits', True, now)

        process_notifications(db_address, redis_client, 'cam1')

        frames = get_stack_frames(db_address, 'mol-drain')
        assert len(frames) == 3
        assert all(f.status == 'complete' for f in frames)
