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


class TestSetupSubscription:

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


class TestCheckReplicationHealth:

    @mock.patch('banzai.dbs.get_session')
    def test_returns_empty_dict_when_no_subscription(self, mock_get_session):
        mock_session = mock.MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_get_session.return_value = mock_session
        mock_session.execute.return_value.fetchone.return_value = None
        assert replication.check_replication_health('postgresql://local/db') == {}
