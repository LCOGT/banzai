import mock
import pytest
from psycopg import errors as pg_errors
from sqlalchemy.exc import OperationalError
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
    def test_generates_sql(self, mock_create_engine):
        engine, conn = _mock_engine()
        mock_create_engine.return_value = engine
        replication.setup_subscription('postgresql+psycopg://local/db', 'host=aws dbname=cal',
                                       slot_name='banzai_lsc_slot')
        sql_arg = conn.execute.call_args[0][0]
        assert isinstance(sql_arg, TextClause)
        assert 'banzai_calibrations_sub' in sql_arg.text
        assert 'banzai_lsc_slot' in sql_arg.text
        assert 'banzai_calibrations' in sql_arg.text
        assert 'create_slot = true' in sql_arg.text

    @mock.patch('banzai.cache.replication.create_engine')
    def test_uses_autocommit(self, mock_create_engine):
        engine, conn = _mock_engine()
        mock_create_engine.return_value = engine
        replication.setup_subscription('postgresql+psycopg://local/db', 'host=aws', slot_name='banzai_ogg_slot')
        conn.execution_options.assert_called_with(isolation_level="AUTOCOMMIT")

    @mock.patch('banzai.cache.replication.create_engine')
    def test_reuses_existing_slot(self, mock_create_engine):
        engine, conn = _mock_engine()
        mock_create_engine.return_value = engine
        orig = pg_errors.ProtocolViolation('replication slot "banzai_lsc_slot" already exists')
        slot_exists_error = OperationalError('', {}, orig)
        conn.execute.side_effect = [slot_exists_error, None]
        replication.setup_subscription('postgresql+psycopg://local/db', 'host=aws dbname=cal',
                                       slot_name='banzai_lsc_slot')
        assert conn.execute.call_count == 2
        retry_sql = conn.execute.call_args_list[1][0][0]
        assert 'create_slot = false' in retry_sql.text

    @mock.patch('banzai.cache.replication.create_engine')
    def test_non_slot_operational_error_propagates(self, mock_create_engine):
        engine, conn = _mock_engine()
        mock_create_engine.return_value = engine
        orig = Exception('connection refused')
        conn.execute.side_effect = OperationalError('', {}, orig)
        with pytest.raises(OperationalError):
            replication.setup_subscription('postgresql+psycopg://local/db', 'host=aws', slot_name='banzai_lsc_slot')
