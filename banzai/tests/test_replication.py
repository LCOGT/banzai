import mock
import pytest
from sqlalchemy.sql.elements import TextClause

from banzai.cache import replication

pytestmark = pytest.mark.replication


def _create_mock_engine_with_autocommit():
    """
    Helper to create a properly mocked engine with AUTOCOMMIT context manager.

    Simulates the SQLAlchemy chain:
        engine.connect().execution_options(isolation_level="AUTOCOMMIT")

    Where:
    1. engine.connect() returns a connection
    2. connection.execution_options() returns the same connection configured as a context manager
    3. The context manager yields the connection for use in the with block

    Returns
    -------
    tuple
        (mock_engine, mock_conn) where:
        - mock_engine: The mock engine object
        - mock_conn: The mock connection used inside the with block
    """
    mock_conn = mock.MagicMock()

    # The connection returned by connect() - calling execution_options() on it
    # returns itself configured as a context manager
    mock_conn.execution_options.return_value = mock_conn
    mock_conn.__enter__ = mock.MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = mock.MagicMock(return_value=False)

    mock_engine = mock.MagicMock()
    mock_engine.connect.return_value = mock_conn

    return mock_engine, mock_conn


class TestSetupSubscription:
    """Tests for setup_subscription function."""

    @mock.patch('banzai.cache.replication.logger')
    @mock.patch('banzai.cache.replication.create_engine')
    def test_logs_error_and_reraises_on_exception(self, mock_create_engine, mock_logger):
        """Verify exception is logged and re-raised."""
        mock_create_engine.side_effect = Exception('Connection failed')

        with pytest.raises(Exception) as exc_info:
            replication.setup_subscription(
                local_db_address='postgresql://localhost/test',
                aws_connection_string='host=aws.rds.amazonaws.com',
                site_id='lsc'
            )

        assert 'Connection failed' in str(exc_info.value)
        mock_logger.error.assert_called_once()
        assert 'Failed to create subscription' in mock_logger.error.call_args[0][0]

    @mock.patch('banzai.cache.replication.create_engine')
    def test_generates_correct_sql_with_site_id(self, mock_create_engine):
        """Verify subscription SQL uses site_id in names and is wrapped with text()."""
        mock_engine, mock_conn = _create_mock_engine_with_autocommit()
        mock_create_engine.return_value = mock_engine

        replication.setup_subscription(
            local_db_address='postgresql://localhost/test',
            aws_connection_string='host=aws.rds.amazonaws.com dbname=cal',
            site_id='lsc'
        )

        # Verify SQL was executed with text() wrapper
        executed_arg = mock_conn.execute.call_args[0][0]
        assert isinstance(executed_arg, TextClause), "SQL should be wrapped with text()"
        executed_sql = executed_arg.text
        assert 'banzai_lsc_sub' in executed_sql
        assert 'banzai_lsc_slot' in executed_sql
        assert 'banzai_calibrations' in executed_sql

    @mock.patch('banzai.cache.replication.create_engine')
    def test_uses_autocommit_isolation_level(self, mock_create_engine):
        """Verify AUTOCOMMIT is used for CREATE SUBSCRIPTION."""
        mock_engine, mock_conn = _create_mock_engine_with_autocommit()
        mock_create_engine.return_value = mock_engine

        replication.setup_subscription(
            local_db_address='postgresql://localhost/test',
            aws_connection_string='host=aws.rds.amazonaws.com',
            site_id='ogg'
        )

        mock_conn.execution_options.assert_called_once_with(isolation_level="AUTOCOMMIT")

    @mock.patch('banzai.cache.replication.create_engine')
    def test_uses_custom_subscription_name(self, mock_create_engine):
        """Verify custom subscription name is used when provided."""
        mock_engine, mock_conn = _create_mock_engine_with_autocommit()
        mock_create_engine.return_value = mock_engine

        replication.setup_subscription(
            local_db_address='postgresql://localhost/test',
            aws_connection_string='host=aws.rds.amazonaws.com',
            site_id='cpt',
            subscription_name='custom_sub',
            slot_name='custom_slot'
        )

        # Verify SQL was executed with text() wrapper
        executed_arg = mock_conn.execute.call_args[0][0]
        assert isinstance(executed_arg, TextClause), "SQL should be wrapped with text()"
        executed_sql = executed_arg.text
        assert 'custom_sub' in executed_sql
        assert 'custom_slot' in executed_sql

    @mock.patch('banzai.cache.replication.create_engine')
    def test_uses_default_publication_name(self, mock_create_engine):
        """Verify default publication name 'banzai_calibrations' is used when not specified."""
        mock_engine, mock_conn = _create_mock_engine_with_autocommit()
        mock_create_engine.return_value = mock_engine

        replication.setup_subscription(
            local_db_address='postgresql://localhost/test',
            aws_connection_string='host=aws.rds.amazonaws.com dbname=cal',
            site_id='lsc'
        )

        executed_arg = mock_conn.execute.call_args[0][0]
        executed_sql = executed_arg.text
        assert 'PUBLICATION banzai_calibrations' in executed_sql

    @mock.patch('banzai.cache.replication.create_engine')
    def test_uses_custom_publication_name(self, mock_create_engine):
        """Verify custom publication name is included in SQL when provided."""
        mock_engine, mock_conn = _create_mock_engine_with_autocommit()
        mock_create_engine.return_value = mock_engine

        replication.setup_subscription(
            local_db_address='postgresql://localhost/test',
            aws_connection_string='host=aws.rds.amazonaws.com dbname=cal',
            site_id='lsc',
            publication_name='my_custom_publication'
        )

        executed_arg = mock_conn.execute.call_args[0][0]
        executed_sql = executed_arg.text
        assert 'PUBLICATION my_custom_publication' in executed_sql
        assert 'banzai_calibrations' not in executed_sql


class TestCheckReplicationHealth:
    """Tests for check_replication_health function."""

    @mock.patch('banzai.cache.replication.logger')
    @mock.patch('banzai.dbs.get_session')
    def test_logs_error_and_reraises_on_exception(self, mock_get_session, mock_logger):
        """Verify exception is logged and re-raised."""
        mock_get_session.side_effect = Exception('Database connection failed')

        with pytest.raises(Exception) as exc_info:
            replication.check_replication_health('postgresql://localhost/test')

        assert 'Database connection failed' in str(exc_info.value)
        mock_logger.error.assert_called_once()
        assert 'Failed to check replication health' in mock_logger.error.call_args[0][0]

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

        # Check that warning was logged for high lag with specific message format
        mock_logger.warning.assert_any_call('Replication lag is high: 400.0 seconds')

    @mock.patch('banzai.cache.replication.logger')
    @mock.patch('banzai.dbs.get_session')
    def test_handles_none_lag_seconds(self, mock_get_session, mock_logger):
        """Verify lag_seconds is None when database returns None (not an error)."""
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.MagicMock(return_value=mock_session)
        mock_session.__exit__ = mock.MagicMock(return_value=False)
        mock_get_session.return_value = mock_session
        # Simulate database returning None for lag_seconds (e.g., no messages received yet)
        mock_session.execute.return_value.fetchone.return_value = (
            'test_sub', 1234, '0/1234', '0/5678',
            '2024-01-01 12:00:00', None,  # last_msg_receipt_time is None
            '2024-01-01 12:00:00', None   # lag_seconds is None
        )

        health = replication.check_replication_health('postgresql://localhost/test')

        # Verify lag_seconds is None (not an error, not converted to 0)
        assert health['lag_seconds'] is None
        # Verify warning logged about undetermined lag
        mock_logger.warning.assert_called_with('Replication lag could not be determined')


class TestInstallTriggers:
    """Tests for install_triggers function."""

    @mock.patch('banzai.cache.replication.logger')
    @mock.patch('banzai.dbs.get_session')
    @mock.patch('os.path.exists', return_value=True)
    def test_logs_error_and_reraises_on_exception(self, mock_exists, mock_get_session, mock_logger):
        """Verify exception is logged and re-raised."""
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.MagicMock(return_value=mock_session)
        mock_session.__exit__ = mock.MagicMock(return_value=False)
        mock_get_session.return_value = mock_session
        mock_session.execute.side_effect = Exception('SQL syntax error')

        with mock.patch('builtins.open', mock.mock_open(read_data='CREATE FUNCTION test();')):
            with pytest.raises(Exception) as exc_info:
                replication.install_triggers('postgresql://localhost/test')

        assert 'SQL syntax error' in str(exc_info.value)
        mock_logger.error.assert_called_once()
        assert 'Failed to install triggers' in mock_logger.error.call_args[0][0]

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
    def test_resolves_relative_paths(self, mock_exists, mock_get_session):
        """Verify relative paths are resolved to project root (3 levels up from replication.py)."""
        import os
        mock_session = mock.MagicMock()
        mock_session.__enter__ = mock.MagicMock(return_value=mock_session)
        mock_session.__exit__ = mock.MagicMock(return_value=False)
        mock_get_session.return_value = mock_session

        replication.install_triggers('postgresql://localhost/test', 'sql/triggers.sql')

        # Verify path is correctly constructed: project_root (3 levels up) + relative path
        # replication.py is in banzai/cache/, so 3 levels up is the project root
        replication_file = os.path.abspath(replication.__file__)
        cache_dir = os.path.dirname(replication_file)  # banzai/cache
        banzai_dir = os.path.dirname(cache_dir)  # banzai
        project_root = os.path.dirname(banzai_dir)  # project root
        expected_path = os.path.join(project_root, 'sql/triggers.sql')

        call_args = mock_exists.call_args[0][0]
        assert call_args == expected_path


class TestDropSubscription:
    """Tests for drop_subscription function."""

    @mock.patch('banzai.cache.replication.logger')
    @mock.patch('banzai.cache.replication.create_engine')
    def test_logs_error_and_reraises_on_exception(self, mock_create_engine, mock_logger):
        """Verify exception is logged and re-raised."""
        mock_create_engine.side_effect = Exception('Connection refused')

        with pytest.raises(Exception) as exc_info:
            replication.drop_subscription('postgresql://localhost/test', 'my_sub')

        assert 'Connection refused' in str(exc_info.value)
        mock_logger.error.assert_called_once()
        assert 'Failed to drop subscription' in mock_logger.error.call_args[0][0]

    @mock.patch('banzai.cache.replication.create_engine')
    def test_generates_correct_drop_sql(self, mock_create_engine):
        """Verify DROP SUBSCRIPTION SQL is generated correctly and wrapped with text()."""
        mock_engine, mock_conn = _create_mock_engine_with_autocommit()
        mock_create_engine.return_value = mock_engine

        replication.drop_subscription('postgresql://localhost/test', 'my_sub', drop_slot=False)

        # Verify SQL was executed with text() wrapper
        executed_arg = mock_conn.execute.call_args[0][0]
        assert isinstance(executed_arg, TextClause), "SQL should be wrapped with text()"
        executed_sql = executed_arg.text
        assert 'DROP SUBSCRIPTION IF EXISTS my_sub' in executed_sql
        assert 'CASCADE' not in executed_sql

    @mock.patch('banzai.cache.replication.create_engine')
    def test_includes_cascade_when_drop_slot_true(self, mock_create_engine):
        """Verify CASCADE included when drop_slot=True and SQL is wrapped with text()."""
        mock_engine, mock_conn = _create_mock_engine_with_autocommit()
        mock_create_engine.return_value = mock_engine

        replication.drop_subscription('postgresql://localhost/test', 'my_sub', drop_slot=True)

        # Verify SQL was executed with text() wrapper
        executed_arg = mock_conn.execute.call_args[0][0]
        assert isinstance(executed_arg, TextClause), "SQL should be wrapped with text()"
        executed_sql = executed_arg.text
        assert 'CASCADE' in executed_sql


class TestGetSubscriptionStatus:
    """Tests for get_subscription_status function."""

    @mock.patch('banzai.cache.replication.logger')
    @mock.patch('banzai.dbs.get_session')
    def test_logs_error_and_reraises_on_exception(self, mock_get_session, mock_logger):
        """Verify exception is logged and re-raised."""
        mock_get_session.side_effect = Exception('Query timeout')

        with pytest.raises(Exception) as exc_info:
            replication.get_subscription_status('postgresql://localhost/test')

        assert 'Query timeout' in str(exc_info.value)
        mock_logger.error.assert_called_once()
        assert 'Failed to get subscription status' in mock_logger.error.call_args[0][0]

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
