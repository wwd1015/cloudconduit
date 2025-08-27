"""Unit tests for ConfigManager."""

import os
import pytest
from unittest.mock import patch, Mock

from cloudconduit.utils.config_manager import ConfigManager


class TestConfigManager:
    """Test ConfigManager functionality."""

    def test_init(self):
        """Test ConfigManager initialization."""
        cm = ConfigManager("test-service")
        assert cm.service_name == "test-service"

    def test_init_default_service_name(self):
        """Test default service name."""
        cm = ConfigManager()
        assert cm.service_name == "cloudconduit"

    @patch('platform.system')
    def test_is_macos_detection(self, mock_system):
        """Test macOS detection."""
        mock_system.return_value = "Darwin"
        cm = ConfigManager()
        assert cm.is_macos is True
        
        mock_system.return_value = "Linux"
        cm = ConfigManager()
        assert cm.is_macos is False

    def test_get_config_value_from_env(self):
        """Test getting config value from environment variable."""
        with patch.dict(os.environ, {'SNOWFLAKE_ACCOUNT': 'test-account'}):
            cm = ConfigManager()
            value = cm.get_config_value('snowflake', 'account')
            assert value == 'test-account'

    def test_get_config_value_from_function_config(self):
        """Test function config takes priority over environment."""
        with patch.dict(os.environ, {'SNOWFLAKE_WAREHOUSE': 'env-warehouse'}):
            cm = ConfigManager()
            config = {'warehouse': 'function-warehouse'}
            value = cm.get_config_value('snowflake', 'warehouse', config)
            assert value == 'function-warehouse'  # Function config wins

    def test_get_config_value_from_default(self):
        """Test getting config value from default config.yaml."""
        cm = ConfigManager()
        # This should get 'your-account' from the default config.yaml
        value = cm.get_config_value('snowflake', 'account')
        assert value == 'your-account'

    @patch('platform.system')
    def test_set_credential_success(self, mock_system):
        """Test setting credential in keychain."""
        mock_system.return_value = "Darwin"
        
        with patch('keyring.set_password') as mock_set:
            cm = ConfigManager()
            cm.set_credential("TEST_KEY", "test-value")
            mock_set.assert_called_once_with("cloudconduit", "test_key", "test-value")

    @patch('platform.system')
    def test_set_credential_not_macos_error(self, mock_system):
        """Test setting credential on non-macOS raises error."""
        mock_system.return_value = "Linux"
        cm = ConfigManager()
        
        with pytest.raises(RuntimeError, match="Keychain storage only available on macOS"):
            cm.set_credential("TEST_KEY", "test-value")

    @patch('platform.system')
    def test_delete_credential_success(self, mock_system):
        """Test deleting credential from keychain."""
        mock_system.return_value = "Darwin"
        
        with patch('keyring.delete_password') as mock_delete:
            cm = ConfigManager()
            cm.delete_credential("TEST_KEY")
            mock_delete.assert_called_once_with("cloudconduit", "test_key")

    @patch('platform.system')
    def test_delete_credential_not_macos(self, mock_system):
        """Test deleting credential on non-macOS raises error."""
        mock_system.return_value = "Linux"
        cm = ConfigManager()
        
        with pytest.raises(RuntimeError, match="Keychain deletion only available on macOS"):
            cm.delete_credential("TEST_KEY")

    @patch('cloudconduit.utils.config_manager.get_current_user')
    def test_get_snowflake_config(self, mock_get_user):
        """Test getting Snowflake configuration."""
        mock_get_user.return_value = "test-user"
        
        with patch.dict(os.environ, {'SNOWFLAKE_PASSWORD': 'test-password'}):
            cm = ConfigManager()
            config = cm.get_snowflake_config()
            
            assert config['user'] == 'test-user'
            assert config['account'] == 'your-account'  # From default config
            assert config['warehouse'] == 'COMPUTE_WH'  # From default config
            assert config['password'] == 'test-password'  # From env

    def test_get_databricks_config(self):
        """Test getting Databricks configuration."""
        with patch.dict(os.environ, {'DATABRICKS_ACCESS_TOKEN': 'test-token'}):
            cm = ConfigManager()
            config = cm.get_databricks_config()
            
            assert config['access_token'] == 'test-token'
            assert config['catalog'] == 'main'  # From default config
            assert config['schema'] == 'default'  # From default config

    def test_get_s3_config(self):
        """Test getting S3 configuration."""
        with patch.dict(os.environ, {'AWS_ACCESS_KEY_ID': 'test-key'}):
            cm = ConfigManager()
            config = cm.get_s3_config()
            
            assert config['aws_access_key_id'] == 'test-key'
            assert config['region_name'] == 'us-east-1'  # From default config

    def test_priority_order(self):
        """Test configuration priority order."""
        cm = ConfigManager()
        
        # 1. Test config.yaml default (lowest priority)
        value = cm.get_config_value('snowflake', 'warehouse')
        assert value == 'COMPUTE_WH'  # From config.yaml
        
        # 2. Test environment variable override
        with patch.dict(os.environ, {'SNOWFLAKE_WAREHOUSE': 'ENV_WH'}):
            value = cm.get_config_value('snowflake', 'warehouse')
            assert value == 'ENV_WH'  # Env overrides default
            
            # 3. Test function parameter override (highest priority)
            function_config = {'warehouse': 'FUNCTION_WH'}
            value = cm.get_config_value('snowflake', 'warehouse', function_config)
            assert value == 'FUNCTION_WH'  # Function overrides env and default