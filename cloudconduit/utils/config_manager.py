"""Configuration manager with simplified priority system for CloudConduit."""

import os
import yaml
from pathlib import Path
from typing import Dict, Optional, Any
import keyring
import platform
from .system_info import get_current_user


class ConfigManager:
    """Manages configuration with the following priority order:
    1. Function parameters (highest priority)
    2. Environment variables
    3. Keychain (passwords/tokens only, macOS only)
    4. config.yaml defaults (lowest priority)
    """

    def __init__(self, service_name: str = "cloudconduit"):
        """Initialize configuration manager.
        
        Args:
            service_name: Service name for keychain storage
        """
        self.service_name = service_name
        self.is_macos = platform.system() == "Darwin"
        self._default_config = None

    def get_default_config_path(self) -> Path:
        """Get path to the default config file in the package."""
        return Path(__file__).parent.parent / "config.yaml"

    def _load_default_config(self) -> Dict[str, Any]:
        """Load default configuration from YAML file (cached)."""
        if self._default_config is None:
            config_path = self.get_default_config_path()
            
            if not config_path.exists():
                self._default_config = {}
            else:
                try:
                    with open(config_path, 'r') as f:
                        self._default_config = yaml.safe_load(f) or {}
                except Exception:
                    # Silently fail for default config loading
                    self._default_config = {}
        
        return self._default_config

    def _get_credential_from_keychain(self, key: str) -> Optional[str]:
        """Get credential from keychain (macOS only).
        
        Args:
            key: Credential key name (e.g., 'SNOWFLAKE_PASSWORD', 'DATABRICKS_ACCESS_TOKEN')
            
        Returns:
            Credential value or None if not found
        """
        if not self.is_macos:
            return None
        
        try:
            return keyring.get_password(self.service_name, key.lower())
        except Exception:
            return None

    def get_config_value(self, service: str, key: str, function_config: Optional[Dict[str, Any]] = None, is_credential: bool = False) -> Optional[str]:
        """Get configuration value following priority order.
        
        Args:
            service: Service name ('snowflake', 'databricks', 's3')
            key: Configuration key
            function_config: Configuration passed to function (highest priority)
            is_credential: Whether this is a credential (password/token)
            
        Returns:
            Configuration value or None
        """
        # 1. Function parameters (highest priority)
        if function_config and key in function_config:
            return function_config[key]

        # 2. Environment variables
        env_key = self._get_env_key(service, key)
        env_value = os.getenv(env_key)
        if env_value:
            return env_value

        # 3. Keychain (credentials only, macOS only)
        if is_credential:
            keychain_value = self._get_credential_from_keychain(env_key)
            if keychain_value:
                return keychain_value

        # 4. Default config.yaml (lowest priority)
        default_config = self._load_default_config()
        service_config = default_config.get(service, {})
        return service_config.get(key)

    def _get_env_key(self, service: str, key: str) -> str:
        """Convert service and key to environment variable name."""
        env_mappings = {
            'snowflake': {
                'account': 'SNOWFLAKE_ACCOUNT',
                'warehouse': 'SNOWFLAKE_WAREHOUSE',
                'database': 'SNOWFLAKE_DATABASE',
                'schema': 'SNOWFLAKE_SCHEMA',
                'user': 'SNOWFLAKE_USER',
                'password': 'SNOWFLAKE_PASSWORD',
                'private_key_path': 'SNOWFLAKE_PRIVATE_KEY_PATH',
                'private_key_passphrase': 'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE',
                'authenticator': 'SNOWFLAKE_AUTHENTICATOR',
            },
            'databricks': {
                'server_hostname': 'DATABRICKS_SERVER_HOSTNAME',
                'http_path': 'DATABRICKS_HTTP_PATH',
                'access_token': 'DATABRICKS_ACCESS_TOKEN',
                'catalog': 'DATABRICKS_CATALOG',
                'schema': 'DATABRICKS_SCHEMA',
            },
            's3': {
                'aws_access_key_id': 'AWS_ACCESS_KEY_ID',
                'aws_secret_access_key': 'AWS_SECRET_ACCESS_KEY',
                'aws_session_token': 'AWS_SESSION_TOKEN',
                'region_name': 'AWS_DEFAULT_REGION',
            }
        }
        
        return env_mappings.get(service, {}).get(key, f"{service.upper()}_{key.upper()}")

    def get_snowflake_config(self, username: Optional[str] = None, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get complete Snowflake configuration following priority order.
        
        Args:
            username: Snowflake username (if None, uses system user)
            config: Function-level configuration overrides
            
        Returns:
            Complete Snowflake configuration dictionary
        """
        # Determine username
        if username is None:
            username = get_current_user()

        # Build configuration following priority order
        result = {
            'user': username,
            'account': self.get_config_value('snowflake', 'account', config),
            'warehouse': self.get_config_value('snowflake', 'warehouse', config),
            'database': self.get_config_value('snowflake', 'database', config),
            'schema': self.get_config_value('snowflake', 'schema', config),
            'password': self.get_config_value('snowflake', 'password', config, is_credential=True),
            'private_key_path': self.get_config_value('snowflake', 'private_key_path', config),
            'private_key_passphrase': self.get_config_value('snowflake', 'private_key_passphrase', config, is_credential=True),
            'authenticator': self.get_config_value('snowflake', 'authenticator', config),
        }
        
        # Remove None values
        return {k: v for k, v in result.items() if v is not None}

    def get_databricks_config(self, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get complete Databricks configuration following priority order.
        
        Args:
            config: Function-level configuration overrides
            
        Returns:
            Complete Databricks configuration dictionary
        """
        result = {
            'server_hostname': self.get_config_value('databricks', 'server_hostname', config),
            'http_path': self.get_config_value('databricks', 'http_path', config),
            'access_token': self.get_config_value('databricks', 'access_token', config, is_credential=True),
            'catalog': self.get_config_value('databricks', 'catalog', config),
            'schema': self.get_config_value('databricks', 'schema', config),
        }
        
        # Remove None values
        return {k: v for k, v in result.items() if v is not None}

    def get_s3_config(self, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get complete S3 configuration following priority order.
        
        Args:
            config: Function-level configuration overrides
            
        Returns:
            Complete S3 configuration dictionary
        """
        result = {
            'aws_access_key_id': self.get_config_value('s3', 'aws_access_key_id', config, is_credential=True),
            'aws_secret_access_key': self.get_config_value('s3', 'aws_secret_access_key', config, is_credential=True),
            'aws_session_token': self.get_config_value('s3', 'aws_session_token', config, is_credential=True),
            'region_name': self.get_config_value('s3', 'region_name', config),
        }
        
        # Remove None values
        return {k: v for k, v in result.items() if v is not None}

    def set_credential(self, key: str, value: str) -> None:
        """Set credential in keychain (macOS only).
        
        Args:
            key: Credential key name (e.g., 'SNOWFLAKE_PASSWORD', 'DATABRICKS_ACCESS_TOKEN')
            value: Credential value
            
        Raises:
            RuntimeError: If not on macOS
        """
        if not self.is_macos:
            raise RuntimeError("Keychain storage only available on macOS")
        
        keyring.set_password(self.service_name, key.lower(), value)

    def delete_credential(self, key: str) -> None:
        """Delete credential from keychain (macOS only).
        
        Args:
            key: Credential key name
            
        Raises:
            RuntimeError: If not on macOS
        """
        if not self.is_macos:
            raise RuntimeError("Keychain deletion only available on macOS")
        
        keyring.delete_password(self.service_name, key.lower())

    def show_config(self) -> None:
        """Display current default configuration."""
        config = self._load_default_config()
        
        if not config:
            print("No default configuration found.")
            return
        
        print("CloudConduit Default Configuration:")
        print("=" * 40)
        
        for service, params in config.items():
            print(f"\n{service.upper()}:")
            for key, value in params.items():
                print(f"  {key}: {value}")