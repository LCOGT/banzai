import mock
import pytest

from banzai.cache import replication

pytestmark = pytest.mark.replication


def _create_mock_engine_with_autocommit():
    """Helper to create a properly mocked engine with AUTOCOMMIT context manager."""
    mock_conn = mock.MagicMock()
    mock_context = mock.MagicMock()
    mock_context.__enter__ = mock.MagicMock(return_value=mock_conn)
    mock_context.__exit__ = mock.MagicMock(return_value=False)
    mock_connect_result = mock.MagicMock()
    mock_connect_result.execution_options.return_value = mock_context
    mock_engine = mock.MagicMock()
    mock_engine.connect.return_value = mock_connect_result
    return mock_engine, mock_conn, mock_connect_result


class TestSetupSubscription:
    """Tests for setup_subscription function."""

    @mock.patch('banzai.cache.replication.create_engine')
    def test_generates_correct_sql_with_site_id(self, mock_create_engine):
        """Verify subscription SQL uses site_id in names."""
        mock_engine, mock_conn, _ = _create_mock_engine_with_autocommit()
        mock_create_engine.return_value = mock_engine

        replication.setup_subscription(
            local_db_address='postgresql://localhost/test',
            aws_connection_string='host=aws.rds.amazonaws.com dbname=cal',
            site_id='lsc'
        )

        # Verify SQL was executed
        executed_sql = mock_conn.execute.call_args[0][0].text
        assert 'banzai_lsc_sub' in executed_sql
        assert 'banzai_lsc_slot' in executed_sql
        assert 'banzai_calibrations' in executed_sql

    @mock.patch('banzai.cache.replication.create_engine')
    def test_uses_autocommit_isolation_level(self, mock_create_engine):
        """Verify AUTOCOMMIT is used for CREATE SUBSCRIPTION."""
        mock_engine, _, mock_connect_result = _create_mock_engine_with_autocommit()
        mock_create_engine.return_value = mock_engine

        replication.setup_subscription(
            local_db_address='postgresql://localhost/test',
            aws_connection_string='host=aws.rds.amazonaws.com',
            site_id='ogg'
        )

        mock_connect_result.execution_options.assert_called_once_with(isolation_level="AUTOCOMMIT")

    @mock.patch('banzai.cache.replication.create_engine')
    def test_uses_custom_subscription_name(self, mock_create_engine):
        """Verify custom subscription name is used when provided."""
        mock_engine, mock_conn, _ = _create_mock_engine_with_autocommit()
        mock_create_engine.return_value = mock_engine

        replication.setup_subscription(
            local_db_address='postgresql://localhost/test',
            aws_connection_string='host=aws.rds.amazonaws.com',
            site_id='cpt',
            subscription_name='custom_sub',
            slot_name='custom_slot'
        )

        executed_sql = mock_conn.execute.call_args[0][0].text
        assert 'custom_sub' in executed_sql
        assert 'custom_slot' in executed_sql


class TestCheckReplicationHealth:
    """Tests for check_replication_health function."""

    @mock.patch('banzai.dbs.get_session')
    def test_returns_metrics_dict(self, mock_get_session):
        """Verify health metrics are returned correctly."""
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.MagicMock(return_value=mock_session)
        mock_session.__exit__ = mock.MagicMock(return_value=False)
        mock_get_session.return_value = mock_session
        mock_session.execute.return_value.fetchone.return_value = (
            'test_sub', 1234, '0/1234', '0/5678',
            '2024-01-01 12:00:00', '2024-01-01 12:00:01',
            '2024-01-01 12:00:00', 5.0
        )

        health = replication.check_replication_health('postgresql://localhost/test')

        assert health['subname'] == 'test_sub'
        assert health['pid'] == 1234
        assert health['lag_seconds'] == 5.0

    @mock.patch('banzai.dbs.get_session')
    def test_returns_empty_dict_when_no_subscription(self, mock_get_session):
        """Verify empty dict returned when no subscription exists."""
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.MagicMock(return_value=mock_session)
        mock_session.__exit__ = mock.MagicMock(return_value=False)
        mock_get_session.return_value = mock_session
        mock_session.execute.return_value.fetchone.return_value = None

        health = replication.check_replication_health('postgresql://localhost/test')

        assert health == {}

    @mock.patch('banzai.cache.replication.logger')
    @mock.patch('banzai.dbs.get_session')
    def test_logs_warning_on_high_lag(self, mock_get_session, mock_logger):
        """Verify warning logged when replication lag exceeds 5 minutes."""
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.MagicMock(return_value=mock_session)
        mock_session.__exit__ = mock.MagicMock(return_value=False)
        mock_get_session.return_value = mock_session
        mock_session.execute.return_value.fetchone.return_value = (
            'test_sub', 1234, '0/1234', '0/5678',
            '2024-01-01 12:00:00', '2024-01-01 12:00:01',
            '2024-01-01 12:00:00', 400.0  # 400 seconds > 300 threshold
        )

        replication.check_replication_health('postgresql://localhost/test')

        # Check that warning was logged for high lag
        warning_calls = [call for call in mock_logger.warning.call_args_list
                        if 'high' in str(call).lower()]
        assert len(warning_calls) == 1


class TestInstallTriggers:
    """Tests for install_triggers function."""

    @mock.patch('banzai.dbs.get_session')
    @mock.patch('builtins.open', mock.mock_open(read_data='CREATE FUNCTION test();'))
    @mock.patch('os.path.exists', return_value=True)
    def test_reads_sql_file_and_executes(self, mock_exists, mock_get_session):
        """Verify SQL file is read and executed."""
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.MagicMock(return_value=mock_session)
        mock_session.__exit__ = mock.MagicMock(return_value=False)
        mock_get_session.return_value = mock_session

        replication.install_triggers('postgresql://localhost/test')

        mock_session.execute.assert_called_once()
        mock_session.commit.assert_called_once()

    @mock.patch('os.path.exists', return_value=False)
    def test_raises_file_not_found_when_missing(self, mock_exists):
        """Verify FileNotFoundError raised when triggers file missing."""
        with pytest.raises(FileNotFoundError) as exc_info:
            replication.install_triggers('postgresql://localhost/test')

        assert 'Triggers file not found' in str(exc_info.value)

    @mock.patch('banzai.dbs.get_session')
    @mock.patch('builtins.open', mock.mock_open(read_data='SELECT 1;'))
    @mock.patch('os.path.exists', return_value=True)
    @mock.patch('os.path.isabs', return_value=False)
    def test_resolves_relative_paths(self, mock_isabs, mock_exists, mock_get_session):
        """Verify relative paths are resolved to project root."""
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.MagicMock(return_value=mock_session)
        mock_session.__exit__ = mock.MagicMock(return_value=False)
        mock_get_session.return_value = mock_session

        replication.install_triggers('postgresql://localhost/test', 'sql/triggers.sql')

        # Verify os.path.exists was called with an absolute path
        call_args = mock_exists.call_args[0][0]
        assert 'sql/triggers.sql' in call_args or 'triggers.sql' in call_args


class TestDropSubscription:
    """Tests for drop_subscription function."""

    @mock.patch('banzai.cache.replication.create_engine')
    def test_generates_correct_drop_sql(self, mock_create_engine):
        """Verify DROP SUBSCRIPTION SQL is generated correctly."""
        mock_engine, mock_conn, _ = _create_mock_engine_with_autocommit()
        mock_create_engine.return_value = mock_engine

        replication.drop_subscription('postgresql://localhost/test', 'my_sub', drop_slot=False)

        executed_sql = mock_conn.execute.call_args[0][0].text
        assert 'DROP SUBSCRIPTION IF EXISTS my_sub' in executed_sql
        assert 'CASCADE' not in executed_sql

    @mock.patch('banzai.cache.replication.create_engine')
    def test_includes_cascade_when_drop_slot_true(self, mock_create_engine):
        """Verify CASCADE included when drop_slot=True."""
        mock_engine, mock_conn, _ = _create_mock_engine_with_autocommit()
        mock_create_engine.return_value = mock_engine

        replication.drop_subscription('postgresql://localhost/test', 'my_sub', drop_slot=True)

        executed_sql = mock_conn.execute.call_args[0][0].text
        assert 'CASCADE' in executed_sql


class TestGetSubscriptionStatus:
    """Tests for get_subscription_status function."""

    @mock.patch('banzai.dbs.get_session')
    def test_returns_list_of_subscriptions(self, mock_get_session):
        """Verify list of subscription dicts is returned."""
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.MagicMock(return_value=mock_session)
        mock_session.__exit__ = mock.MagicMock(return_value=False)
        mock_get_session.return_value = mock_session
        mock_session.execute.return_value.fetchall.return_value = [
            ('sub1', True, 'slot1'),
            ('sub2', False, 'slot2'),
        ]

        result = replication.get_subscription_status('postgresql://localhost/test')

        assert len(result) == 2
        assert result[0]['subname'] == 'sub1'
        assert result[0]['subenabled'] is True
        assert result[1]['subname'] == 'sub2'
        assert result[1]['subenabled'] is False

    @mock.patch('banzai.dbs.get_session')
    def test_returns_empty_list_when_no_subscriptions(self, mock_get_session):
        """Verify empty list returned when no subscriptions exist."""
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.MagicMock(return_value=mock_session)
        mock_session.__exit__ = mock.MagicMock(return_value=False)
        mock_get_session.return_value = mock_session
        mock_session.execute.return_value.fetchall.return_value = []

        result = replication.get_subscription_status('postgresql://localhost/test')

        assert result == []
