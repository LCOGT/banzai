import os

import mock
import pytest

from banzai.cache import init

pytestmark = pytest.mark.cache_init


class TestIsAlreadyInitialized:
    @mock.patch('banzai.dbs.get_cache_config')
    def test_returns_true_when_config_exists(self, mock_get_config):
        mock_get_config.return_value = mock.MagicMock()
        result = init.is_already_initialized('sqlite:///test.db')
        assert result is True

    @mock.patch('banzai.dbs.get_cache_config')
    def test_returns_false_when_config_is_none(self, mock_get_config):
        mock_get_config.return_value = None
        result = init.is_already_initialized('sqlite:///test.db')
        assert result is False

    @mock.patch('banzai.dbs.get_cache_config')
    def test_returns_false_on_exception(self, mock_get_config):
        mock_get_config.side_effect = Exception("Table does not exist")
        result = init.is_already_initialized('sqlite:///test.db')
        assert result is False


class TestCheckSubscriptionExists:
    @mock.patch('banzai.cache.replication.get_subscription_status')
    def test_returns_true_when_subscriptions_exist(self, mock_get_status):
        mock_get_status.return_value = [{'subname': 'test_sub'}]
        result = init.check_subscription_exists('sqlite:///test.db')
        assert result is True

    @mock.patch('banzai.cache.replication.get_subscription_status')
    def test_returns_false_when_empty_list(self, mock_get_status):
        mock_get_status.return_value = []
        result = init.check_subscription_exists('sqlite:///test.db')
        assert result is False

    @mock.patch('banzai.cache.replication.get_subscription_status')
    def test_returns_false_on_exception(self, mock_get_status):
        mock_get_status.side_effect = Exception("Connection failed")
        result = init.check_subscription_exists('sqlite:///test.db')
        assert result is False


class TestRunInitialization:
    @mock.patch.dict(os.environ, {}, clear=True)
    def test_exits_with_code_1_when_db_address_missing(self):
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 1

    @mock.patch.dict(os.environ, {'DB_ADDRESS': 'sqlite:///test.db'}, clear=True)
    def test_exits_with_code_1_when_site_id_missing(self):
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 1

    @mock.patch('banzai.cache.init.is_already_initialized', return_value=True)
    @mock.patch.dict(os.environ, {'DB_ADDRESS': 'sqlite:///test.db', 'SITE_ID': 'lsc'}, clear=True)
    def test_exits_with_code_0_when_already_initialized(self, mock_is_init):
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 0

    @mock.patch('banzai.dbs.initialize_cache_config')
    @mock.patch('banzai.cache.replication.install_triggers')
    @mock.patch('banzai.dbs.create_db')
    @mock.patch('banzai.cache.init.is_already_initialized', return_value=False)
    @mock.patch.dict(os.environ, {'DB_ADDRESS': 'sqlite:///test.db', 'SITE_ID': 'lsc'}, clear=True)
    def test_runs_all_steps_in_order_without_aws(self, mock_is_init, mock_create_db,
                                                  mock_install_triggers, mock_init_config):
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 0
        mock_create_db.assert_called_once_with('sqlite:///test.db')
        mock_install_triggers.assert_called_once_with('sqlite:///test.db')
        mock_init_config.assert_called_once()

    @mock.patch('banzai.dbs.initialize_cache_config')
    @mock.patch('banzai.cache.replication.install_triggers')
    @mock.patch('banzai.cache.replication.setup_subscription')
    @mock.patch('banzai.cache.init.check_subscription_exists', return_value=False)
    @mock.patch('banzai.dbs.create_db')
    @mock.patch('banzai.cache.init.is_already_initialized', return_value=False)
    @mock.patch.dict(os.environ, {
        'DB_ADDRESS': 'sqlite:///test.db',
        'AWS_DB_ADDRESS': 'postgresql://aws/db',
        'SITE_ID': 'lsc'
    }, clear=True)
    def test_runs_all_steps_with_aws_replication(self, mock_is_init, mock_create_db,
                                                  mock_check_sub, mock_setup_sub,
                                                  mock_install_triggers, mock_init_config):
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 0
        mock_create_db.assert_called_once()
        mock_setup_sub.assert_called_once()
        mock_install_triggers.assert_called_once()
        mock_init_config.assert_called_once()

    @mock.patch('banzai.dbs.initialize_cache_config')
    @mock.patch('banzai.cache.replication.install_triggers')
    @mock.patch('banzai.cache.init.check_subscription_exists', return_value=True)
    @mock.patch('banzai.dbs.create_db')
    @mock.patch('banzai.cache.init.is_already_initialized', return_value=False)
    @mock.patch.dict(os.environ, {
        'DB_ADDRESS': 'sqlite:///test.db',
        'AWS_DB_ADDRESS': 'postgresql://aws/db',
        'SITE_ID': 'lsc'
    }, clear=True)
    def test_skips_subscription_when_already_exists(self, mock_is_init, mock_create_db,
                                                     mock_check_sub, mock_install_triggers,
                                                     mock_init_config):
        with mock.patch('banzai.cache.replication.setup_subscription') as mock_setup_sub:
            with pytest.raises(SystemExit) as exc_info:
                init.run_initialization()
            assert exc_info.value.code == 0
            mock_setup_sub.assert_not_called()

    @mock.patch('banzai.dbs.create_db')
    @mock.patch('banzai.cache.init.is_already_initialized', return_value=False)
    @mock.patch.dict(os.environ, {'DB_ADDRESS': 'sqlite:///test.db', 'SITE_ID': 'lsc'}, clear=True)
    def test_exits_with_code_1_on_exception(self, mock_is_init, mock_create_db):
        mock_create_db.side_effect = Exception("Database error")
        with pytest.raises(SystemExit) as exc_info:
            init.run_initialization()
        assert exc_info.value.code == 1
