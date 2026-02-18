import mock
import pytest
from sqlalchemy.sql.elements import TextClause

from banzai.cache import replication

pytestmark = pytest.mark.replication


def _mock_engine():
    """Create mocked SQLAlchemy engine with AUTOCOMMIT context manager."""
    mock_conn = mock.MagicMock()
    mock_conn.execution_options.return_value = mock_conn
    mock_conn.__enter__ = mock.MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = mock.MagicMock(return_value=False)
    mock_engine = mock.MagicMock()
    mock_engine.connect.return_value = mock_conn
    return mock_engine, mock_conn


@pytest.fixture
def mock_session():
    """Mock dbs.get_session context manager returning a mock session."""
    session = mock.MagicMock()
    session.__enter__ = mock.MagicMock(return_value=session)
    session.__exit__ = mock.MagicMock(return_value=False)
    return session


class TestSetupSubscription:

    @mock.patch('banzai.cache.replication.logger')
    @mock.patch('banzai.cache.replication.create_engine')
    def test_logs_error_and_reraises(self, mock_create_engine, mock_logger):
        mock_create_engine.side_effect = Exception('Connection failed')
        with pytest.raises(Exception, match='Connection failed'):
            replication.setup_subscription('postgresql://local/db', 'host=aws', site_id='lsc')
        assert 'Failed to create subscription' in mock_logger.error.call_args[0][0]

    @mock.patch('banzai.cache.replication.create_engine')
    def test_generates_sql_with_site_id(self, mock_create_engine):
        engine, conn = _mock_engine()
        mock_create_engine.return_value = engine
        replication.setup_subscription('postgresql://local/db', 'host=aws dbname=cal', site_id='lsc')
        sql_arg = conn.execute.call_args[0][0]
        assert isinstance(sql_arg, TextClause)
        assert 'banzai_lsc_sub' in sql_arg.text
        assert 'banzai_lsc_slot' in sql_arg.text
        assert 'banzai_calibrations' in sql_arg.text

    @mock.patch('banzai.cache.replication.create_engine')
    def test_uses_autocommit(self, mock_create_engine):
        engine, conn = _mock_engine()
        mock_create_engine.return_value = engine
        replication.setup_subscription('postgresql://local/db', 'host=aws', site_id='ogg')
        conn.execution_options.assert_called_once_with(isolation_level="AUTOCOMMIT")

    @mock.patch('banzai.cache.replication.create_engine')
    def test_custom_names(self, mock_create_engine):
        engine, conn = _mock_engine()
        mock_create_engine.return_value = engine
        replication.setup_subscription(
            'postgresql://local/db', 'host=aws', site_id='cpt',
            subscription_name='custom_sub', slot_name='custom_slot'
        )
        sql = conn.execute.call_args[0][0].text
        assert 'custom_sub' in sql
        assert 'custom_slot' in sql

    @mock.patch('banzai.cache.replication.create_engine')
    def test_custom_publication_name(self, mock_create_engine):
        engine, conn = _mock_engine()
        mock_create_engine.return_value = engine
        replication.setup_subscription(
            'postgresql://local/db', 'host=aws', site_id='lsc',
            publication_name='my_pub'
        )
        sql = conn.execute.call_args[0][0].text
        assert 'PUBLICATION my_pub' in sql
        assert 'banzai_calibrations' not in sql


class TestCheckReplicationHealth:

    @mock.patch('banzai.cache.replication.logger')
    @mock.patch('banzai.dbs.get_session')
    def test_logs_error_and_reraises(self, mock_get_session, mock_logger):
        mock_get_session.side_effect = Exception('Database connection failed')
        with pytest.raises(Exception, match='Database connection failed'):
            replication.check_replication_health('postgresql://local/db')
        assert 'Failed to check replication health' in mock_logger.error.call_args[0][0]

    @mock.patch('banzai.dbs.get_session')
    def test_returns_metrics(self, mock_get_session, mock_session):
        mock_get_session.return_value = mock_session
        mock_session.execute.return_value.fetchone.return_value = (
            'test_sub', 1234, '0/1234', '0/5678',
            '2024-01-01 12:00:00', '2024-01-01 12:00:01',
            '2024-01-01 12:00:00', 5.0
        )
        health = replication.check_replication_health('postgresql://local/db')
        assert health['subname'] == 'test_sub'
        assert health['pid'] == 1234
        assert health['lag_seconds'] == 5.0

    @mock.patch('banzai.dbs.get_session')
    def test_returns_empty_dict_when_no_subscription(self, mock_get_session, mock_session):
        mock_get_session.return_value = mock_session
        mock_session.execute.return_value.fetchone.return_value = None
        assert replication.check_replication_health('postgresql://local/db') == {}

    @mock.patch('banzai.cache.replication.logger')
    @mock.patch('banzai.dbs.get_session')
    def test_logs_warning_on_high_lag(self, mock_get_session, mock_logger, mock_session):
        mock_get_session.return_value = mock_session
        mock_session.execute.return_value.fetchone.return_value = (
            'test_sub', 1234, '0/1234', '0/5678',
            '2024-01-01 12:00:00', '2024-01-01 12:00:01',
            '2024-01-01 12:00:00', 400.0
        )
        replication.check_replication_health('postgresql://local/db')
        mock_logger.warning.assert_any_call('Replication lag is high: 400.0 seconds')

    @mock.patch('banzai.cache.replication.logger')
    @mock.patch('banzai.dbs.get_session')
    def test_handles_none_lag(self, mock_get_session, mock_logger, mock_session):
        mock_get_session.return_value = mock_session
        mock_session.execute.return_value.fetchone.return_value = (
            'test_sub', 1234, '0/1234', '0/5678',
            '2024-01-01 12:00:00', None, '2024-01-01 12:00:00', None
        )
        health = replication.check_replication_health('postgresql://local/db')
        assert health['lag_seconds'] is None
        mock_logger.warning.assert_called_with('Replication lag could not be determined')


class TestDropSubscription:

    @mock.patch('banzai.cache.replication.logger')
    @mock.patch('banzai.cache.replication.create_engine')
    def test_logs_error_and_reraises(self, mock_create_engine, mock_logger):
        mock_create_engine.side_effect = Exception('Connection refused')
        with pytest.raises(Exception, match='Connection refused'):
            replication.drop_subscription('postgresql://local/db', 'my_sub')
        assert 'Failed to drop subscription' in mock_logger.error.call_args[0][0]

    @mock.patch('banzai.cache.replication.create_engine')
    def test_drop_without_cascade(self, mock_create_engine):
        engine, conn = _mock_engine()
        mock_create_engine.return_value = engine
        replication.drop_subscription('postgresql://local/db', 'my_sub', drop_slot=False)
        sql = conn.execute.call_args[0][0]
        assert isinstance(sql, TextClause)
        assert 'DROP SUBSCRIPTION IF EXISTS my_sub' in sql.text
        assert 'CASCADE' not in sql.text

    @mock.patch('banzai.cache.replication.create_engine')
    def test_drop_with_cascade(self, mock_create_engine):
        engine, conn = _mock_engine()
        mock_create_engine.return_value = engine
        replication.drop_subscription('postgresql://local/db', 'my_sub', drop_slot=True)
        assert 'CASCADE' in conn.execute.call_args[0][0].text


class TestGetSubscriptionStatus:

    @mock.patch('banzai.cache.replication.logger')
    @mock.patch('banzai.dbs.get_session')
    def test_logs_error_and_reraises(self, mock_get_session, mock_logger):
        mock_get_session.side_effect = Exception('Query timeout')
        with pytest.raises(Exception, match='Query timeout'):
            replication.get_subscription_status('postgresql://local/db')
        assert 'Failed to get subscription status' in mock_logger.error.call_args[0][0]

    @mock.patch('banzai.dbs.get_session')
    def test_returns_subscriptions(self, mock_get_session, mock_session):
        mock_get_session.return_value = mock_session
        mock_session.execute.return_value.fetchall.return_value = [
            ('sub1', True, 'slot1'),
            ('sub2', False, 'slot2'),
        ]
        result = replication.get_subscription_status('postgresql://local/db')
        assert len(result) == 2
        assert result[0] == {'subname': 'sub1', 'subenabled': True, 'subslotname': 'slot1'}
        assert result[1]['subenabled'] is False

    @mock.patch('banzai.dbs.get_session')
    def test_returns_empty_list_when_none(self, mock_get_session, mock_session):
        mock_get_session.return_value = mock_session
        mock_session.execute.return_value.fetchall.return_value = []
        assert replication.get_subscription_status('postgresql://local/db') == []
