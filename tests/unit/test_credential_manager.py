"""Unit tests for CredentialManager."""

import os
import pytest
from unittest.mock import patch, Mock

from cloudconduit.utils.credential_manager import CredentialManager


class TestCredentialManager:
    """Test CredentialManager functionality."""

    def test_init(self):
        """Test CredentialManager initialization."""
        cm = CredentialManager("test-service")
        assert cm.service_name == "test-service"

    def test_init_default_service_name(self):
        """Test default service name."""
        cm = CredentialManager()
        assert cm.service_name == "cloudconduit"

    @patch('platform.system')
    def test_is_macos_detection(self, mock_system):
        """Test macOS detection."""
        mock_system.return_value = "Darwin"
        cm = CredentialManager()
        assert cm.is_macos is True
        
        mock_system.return_value = "Linux"
        cm = CredentialManager()
        assert cm.is_macos is False

    def test_get_credential_from_env(self, mock_environment):
        """Test getting credential from environment variable."""
        cm = CredentialManager()
        password = cm.get_credential("SNOWFLAKE_PASSWORD")
        assert password == "test-password"

    def test_get_credential_env_priority(self, mock_environment, mock_keyring):
        """Test environment variable takes priority over keychain."""
        cm = CredentialManager()
        password = cm.get_credential("SNOWFLAKE_PASSWORD")
        assert password == "test-password"  # From env, not keychain

    @patch('platform.system')
    def test_get_credential_from_keychain(self, mock_system, mock_keyring):
        """Test getting credential from keychain when env var not set."""
        mock_system.return_value = "Darwin"
        cm = CredentialManager()
        
        with patch.dict(os.environ, {}, clear=True):
            password = cm.get_credential("TEST_KEY")
            assert password == "mock-password"
            mock_keyring['get_password'].assert_called_once_with("cloudconduit", "test_key")

    def test_get_credential_not_found(self):
        """Test credential not found."""
        cm = CredentialManager()
        
        with patch.dict(os.environ, {}, clear=True), \
             patch('keyring.get_password', side_effect=Exception("Not found")):
            password = cm.get_credential("NONEXISTENT_KEY")
            assert password is None

    @patch('platform.system')
    def test_set_credential_success(self, mock_system, mock_keyring):
        """Test setting credential in keychain."""
        mock_system.return_value = "Darwin"
        cm = CredentialManager()
        
        cm.set_credential("TEST_KEY", "test-value")
        mock_keyring['set_password'].assert_called_once_with(
            "cloudconduit", "test_key", "test-value"
        )

    @patch('platform.system')
    def test_set_credential_not_macos_error(self, mock_system):
        """Test setting credential on non-macOS raises error."""
        mock_system.return_value = "Linux"
        cm = CredentialManager()
        
        with pytest.raises(RuntimeError, match="Keychain storage only available on macOS"):
            cm.set_credential("TEST_KEY", "test-value")


    @patch('platform.system')
    def test_delete_credential_success(self, mock_system, mock_keyring):
        """Test deleting credential from keychain."""
        mock_system.return_value = "Darwin"
        cm = CredentialManager()
        
        cm.delete_credential("TEST_KEY")
        mock_keyring['delete_password'].assert_called_once_with("cloudconduit", "test_key")

    @patch('platform.system')
    def test_delete_credential_not_macos(self, mock_system):
        """Test deleting credential on non-macOS raises error."""
        mock_system.return_value = "Linux"
        cm = CredentialManager()
        
        with pytest.raises(RuntimeError, match="Keychain deletion only available on macOS"):
            cm.delete_credential("TEST_KEY")

    @patch('cloudconduit.utils.credential_manager.get_current_user')
    def test_get_snowflake_credentials(self, mock_get_user, mock_environment):
        """Test getting Snowflake credentials."""
        mock_get_user.return_value = "test-user"
        cm = CredentialManager()
        
        creds = cm.get_snowflake_credentials()
        
        expected = {
            'account': 'test-account',
            'user': 'test-user',
            'password': 'test-password',
            'warehouse': 'test-warehouse',
            'database': 'test-database',
            'schema': 'test-schema'
        }
        
        assert creds == expected

    @patch('cloudconduit.utils.credential_manager.get_current_user')
    def test_get_snowflake_credentials_private_key(self, mock_get_user):
        """Test Snowflake credentials with private key."""
        mock_get_user.return_value = "test-user"
        env_vars = {
            'SNOWFLAKE_PRIVATE_KEY_PATH': '/path/to/key.p8',
            'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE': 'passphrase'
        }
        
        with patch.dict(os.environ, env_vars):
            cm = CredentialManager()
            creds = cm.get_snowflake_credentials()
            
            assert 'private_key_path' in creds
            assert 'private_key_passphrase' in creds
            assert 'password' not in creds

    def test_get_databricks_credentials(self, mock_environment):
        """Test getting Databricks credentials."""
        cm = CredentialManager()
        
        creds = cm.get_databricks_credentials()
        
        expected = {
            'server_hostname': 'test-workspace.cloud.databricks.com',
            'http_path': '/sql/1.0/warehouses/test-warehouse',
            'access_token': 'test-token',
            'catalog': 'test-catalog',
            'schema': 'test-schema'
        }
        
        assert creds == expected

    def test_get_s3_credentials(self, mock_environment):
        """Test getting S3 credentials."""
        cm = CredentialManager()
        
        creds = cm.get_s3_credentials()
        
        expected = {
            'aws_access_key_id': 'test-access-key',
            'aws_secret_access_key': 'test-secret-key',
            'aws_session_token': None,
            'region_name': 'us-east-1'
        }
        
        assert creds == expected

    def test_get_s3_credentials_with_defaults(self):
        """Test S3 credentials with defaults."""
        with patch.dict(os.environ, {}, clear=True):
            cm = CredentialManager()
            creds = cm.get_s3_credentials()
            
            # Should have default region
            assert creds['region_name'] == 'us-east-1'
            assert creds['aws_access_key_id'] is None