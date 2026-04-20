import mock
import pytest
from sqlalchemy.exc import ProgrammingError

from banzai.cache import init

pytestmark = pytest.mark.cache_init

AWS_ARGV = (
    'banzai_cache_init',
    '--db-address=sqlite:///test.db', '--aws-db-address=postgresql+psycopg://aws/db',
    '--site-id=lsc',
)


class TestRunInitialization:

    @mock.patch('sys.argv', ['banzai_cache_init'])
    def test_exits_2_when_db_address_missing(self):
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 2

    @mock.patch('sys.argv', [
        'banzai_cache_init', '--db-address=sqlite:///test.db', '--aws-db-address=postgresql+psycopg://aws/db'
    ])
    def test_exits_1_when_site_id_missing_with_aws(self):
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 1

    @mock.patch('banzai.dbs.create_db')
    @mock.patch('sys.argv', ['banzai_cache_init', '--db-address=sqlite:///test.db'])
    def test_happy_path_without_aws(self, mock_create_db):
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 0
        mock_create_db.assert_called_once_with('sqlite:///test.db')

    @mock.patch('banzai.cache.replication.setup_subscription')
    @mock.patch('banzai.dbs.create_db')
    @mock.patch('sys.argv', AWS_ARGV)
    def test_happy_path_with_aws(self, mock_create_db, mock_setup_sub):
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 0
        mock_create_db.assert_called_once_with('sqlite:///test.db')
        mock_setup_sub.assert_called_once_with(
            'sqlite:///test.db', 'postgresql+psycopg://aws/db',
            slot_name='banzai_lsc_slot', publication_name='banzai_calibrations',
        )

    @mock.patch('banzai.cache.replication.setup_subscription')
    @mock.patch('banzai.dbs.create_db')
    @mock.patch('sys.argv', AWS_ARGV)
    def test_subscription_already_exists_continues(self, mock_create_db, mock_setup_sub):
        mock_setup_sub.side_effect = ProgrammingError('', {}, Exception('subscription already exists'))
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 0

    @mock.patch('banzai.cache.replication.setup_subscription')
    @mock.patch('banzai.dbs.create_db')
    @mock.patch('sys.argv', AWS_ARGV)
    def test_subscription_non_duplicate_error_exits_1(self, mock_create_db, mock_setup_sub):
        mock_setup_sub.side_effect = Exception('connection refused')
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 1

    @mock.patch('banzai.dbs.create_db')
    @mock.patch('sys.argv', ['banzai_cache_init', '--db-address=sqlite:///test.db'])
    def test_create_db_failure_exits_1(self, mock_create_db):
        mock_create_db.side_effect = Exception('Database error')
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 1

    @mock.patch('banzai.cache.replication.setup_subscription')
    @mock.patch('banzai.dbs.create_db')
    @mock.patch('sys.argv', list(AWS_ARGV) + ['--publication-name=custom_pub'])
    def test_publication_name_passed_through(self, mock_create_db, mock_setup_sub):
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 0
        mock_setup_sub.assert_called_once_with(
            'sqlite:///test.db', 'postgresql+psycopg://aws/db',
            slot_name='banzai_lsc_slot', publication_name='custom_pub',
        )

    @mock.patch('banzai.cache.replication.setup_subscription')
    @mock.patch('banzai.dbs.create_db')
    @mock.patch('sys.argv', list(AWS_ARGV) + ['--slot-name=custom_slot'])
    def test_slot_name_passed_through(self, mock_create_db, mock_setup_sub):
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 0
        mock_setup_sub.assert_called_once_with(
            'sqlite:///test.db', 'postgresql+psycopg://aws/db',
            slot_name='custom_slot', publication_name='banzai_calibrations',
        )
