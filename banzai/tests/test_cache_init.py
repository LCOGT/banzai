import os

import mock
import pytest
from sqlalchemy.exc import ProgrammingError

from banzai.cache import init

pytestmark = pytest.mark.cache_init

AWS_ENV = {
    'DB_ADDRESS': 'sqlite:///test.db', 'AWS_DB_ADDRESS': 'postgresql://aws/db',
    'SITE_ID': 'lsc',
}


class TestRunInitialization:

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_exits_1_when_db_address_missing(self):
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 1

    @mock.patch.dict(os.environ, {
        'DB_ADDRESS': 'sqlite:///test.db', 'AWS_DB_ADDRESS': 'postgresql://aws/db'
    }, clear=True)
    def test_exits_1_when_site_id_missing_with_aws(self):
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 1

    @mock.patch('banzai.dbs.create_db')
    @mock.patch.dict(os.environ, {'DB_ADDRESS': 'sqlite:///test.db'}, clear=True)
    def test_happy_path_without_aws(self, mock_create_db):
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 0
        mock_create_db.assert_called_once_with('sqlite:///test.db')

    @mock.patch('banzai.cache.replication.setup_subscription')
    @mock.patch('banzai.dbs.create_db')
    @mock.patch.dict(os.environ, AWS_ENV, clear=True)
    def test_happy_path_with_aws(self, mock_create_db, mock_setup_sub):
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 0
        mock_create_db.assert_called_once_with('sqlite:///test.db')
        mock_setup_sub.assert_called_once_with(
            'sqlite:///test.db', 'postgresql://aws/db', site_id='lsc'
        )

    @mock.patch('banzai.cache.replication.setup_subscription')
    @mock.patch('banzai.dbs.create_db')
    @mock.patch.dict(os.environ, AWS_ENV, clear=True)
    def test_subscription_already_exists_continues(self, mock_create_db, mock_setup_sub):
        mock_setup_sub.side_effect = ProgrammingError('', {}, Exception('subscription already exists'))
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 0

    @mock.patch('banzai.cache.replication.setup_subscription')
    @mock.patch('banzai.dbs.create_db')
    @mock.patch.dict(os.environ, AWS_ENV, clear=True)
    def test_subscription_non_duplicate_error_exits_1(self, mock_create_db, mock_setup_sub):
        mock_setup_sub.side_effect = Exception('connection refused')
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 1

    @mock.patch('banzai.dbs.create_db')
    @mock.patch.dict(os.environ, {'DB_ADDRESS': 'sqlite:///test.db'}, clear=True)
    def test_create_db_failure_exits_1(self, mock_create_db):
        mock_create_db.side_effect = Exception('Database error')
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 1
