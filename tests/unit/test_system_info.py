"""Unit tests for system info utilities."""

import pytest
from unittest.mock import patch

from cloudconduit.utils.system_info import (
    get_current_user,
    get_computer_name,
    get_system_info,
    generate_default_user_id,
    get_default_snowflake_user,
    format_keychain_account
)


class TestSystemInfo:
    """Test system information utilities."""

    @patch('getpass.getuser')
    def test_get_current_user_getpass(self, mock_getuser):
        """Test get_current_user using getpass.getuser."""
        mock_getuser.return_value = "john.doe"
        assert get_current_user() == "john.doe"

    @patch('getpass.getuser')
    @patch('os.getlogin')
    def test_get_current_user_fallback_getlogin(self, mock_getlogin, mock_getuser):
        """Test get_current_user fallback to os.getlogin."""
        mock_getuser.side_effect = Exception("getuser failed")
        mock_getlogin.return_value = "jane.smith"
        assert get_current_user() == "jane.smith"

    @patch('getpass.getuser')
    @patch('os.getlogin')
    @patch.dict('os.environ', {'USER': 'env_user'})
    def test_get_current_user_fallback_env(self, mock_getlogin, mock_getuser):
        """Test get_current_user fallback to environment variables."""
        mock_getuser.side_effect = Exception("getuser failed")
        mock_getlogin.side_effect = Exception("getlogin failed")
        assert get_current_user() == "env_user"

    @patch('getpass.getuser')
    @patch('os.getlogin')
    @patch.dict('os.environ', {}, clear=True)
    def test_get_current_user_ultimate_fallback(self, mock_getlogin, mock_getuser):
        """Test get_current_user ultimate fallback."""
        mock_getuser.side_effect = Exception("getuser failed")
        mock_getlogin.side_effect = Exception("getlogin failed")
        assert get_current_user() == "unknown_user"

    @patch('socket.gethostname')
    def test_get_computer_name(self, mock_hostname):
        """Test get_computer_name."""
        mock_hostname.return_value = "my-macbook"
        assert get_computer_name() == "my-macbook"

    @patch('socket.gethostname')
    def test_get_computer_name_fallback(self, mock_hostname):
        """Test get_computer_name fallback."""
        mock_hostname.side_effect = Exception("hostname failed")
        assert get_computer_name() == "unknown_host"

    @patch('cloudconduit.utils.system_info.get_current_user')
    @patch('cloudconduit.utils.system_info.get_computer_name')
    def test_get_system_info(self, mock_computer, mock_user):
        """Test get_system_info."""
        mock_user.return_value = "testuser"
        mock_computer.return_value = "testhost"
        
        info = get_system_info()
        
        assert info["username"] == "testuser"
        assert info["hostname"] == "testhost"
        assert "platform" in info
        assert "python_version" in info

    @patch('cloudconduit.utils.system_info.get_current_user')
    def test_generate_default_user_id_basic(self, mock_user):
        """Test generate_default_user_id basic functionality."""
        mock_user.return_value = "John Doe"
        assert generate_default_user_id() == "john.doe"

    @patch('cloudconduit.utils.system_info.get_current_user')
    def test_generate_default_user_id_with_domain(self, mock_user):
        """Test generate_default_user_id with existing domain."""
        mock_user.return_value = "john.doe@company.com"
        assert generate_default_user_id() == "john.doe"

    @patch('cloudconduit.utils.system_info.get_current_user')
    def test_generate_default_user_id_with_suffix(self, mock_user):
        """Test generate_default_user_id with domain suffix."""
        mock_user.return_value = "john.doe"
        assert generate_default_user_id(domain_suffix="@company.com") == "john.doe@company.com"
        assert generate_default_user_id(domain_suffix="company.com") == "john.doe@company.com"

    @patch('cloudconduit.utils.system_info.get_current_user')
    def test_generate_default_user_id_with_service(self, mock_user):
        """Test generate_default_user_id with service parameter."""
        mock_user.return_value = "john.doe"
        # Service parameter doesn't change the output in current implementation
        assert generate_default_user_id("snowflake") == "john.doe"

    @patch('cloudconduit.utils.system_info.get_current_user')
    def test_get_default_snowflake_user(self, mock_user):
        """Test get_default_snowflake_user."""
        mock_user.return_value = "john.doe"
        assert get_default_snowflake_user() == "john.doe"
        assert get_default_snowflake_user("@company.com") == "john.doe@company.com"

    @patch('cloudconduit.utils.system_info.get_current_user')
    def test_format_keychain_account_default_user(self, mock_user):
        """Test format_keychain_account with default user."""
        mock_user.return_value = "john.doe"
        assert format_keychain_account("snowflake") == "snowflake.john.doe"

    def test_format_keychain_account_custom_user(self):
        """Test format_keychain_account with custom user."""
        assert format_keychain_account("snowflake", "custom.user") == "snowflake.custom.user"

    def test_format_keychain_account_normalize_user(self):
        """Test format_keychain_account normalizes username."""
        result = format_keychain_account("databricks", "John Doe@Company.com")
        assert result == "databricks.john.doe.company.com"

    @patch('cloudconduit.utils.system_info.get_current_user')
    def test_format_keychain_account_spaces_and_caps(self, mock_user):
        """Test format_keychain_account handles spaces and capitals."""
        mock_user.return_value = "John DOE"
        assert format_keychain_account("s3") == "s3.john.doe"