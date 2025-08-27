"""Credential management for macOS keychain and environment variables."""

import os
import platform
from typing import Dict, Optional, Any
import keyring
from dotenv import load_dotenv
from .system_info import format_keychain_account, get_current_user


class CredentialManager:
    """Manages credentials from keychain (macOS) and environment variables."""

    def __init__(self, service_name: str = "cloudconduit"):
        """Initialize credential manager.
        
        Args:
            service_name: Service name for keychain storage
        """
        self.service_name = service_name
        self.is_macos = platform.system() == "Darwin"
        load_dotenv()

    def get_credential(self, key: str) -> Optional[str]:
        """Get credential from environment variables or keychain.
        
        Args:
            key: Credential key name (e.g., 'SNOWFLAKE_PASSWORD', 'DATABRICKS_TOKEN')
            
        Returns:
            Credential value or None if not found
        """
        # First try environment variable
        env_value = os.getenv(key.upper())
        if env_value:
            return env_value

        # Then try keychain on macOS using key as account name
        if self.is_macos:
            try:
                return keyring.get_password(self.service_name, key.lower())
            except Exception:
                pass

        return None

    def set_credential(self, key: str, value: str) -> None:
        """Set credential in keychain (macOS only).
        
        Args:
            key: Credential key name (e.g., 'SNOWFLAKE_PASSWORD', 'DATABRICKS_TOKEN')
            value: Credential value
        """
        if self.is_macos:
            try:
                keyring.set_password(self.service_name, key.lower(), value)
            except Exception as e:
                raise RuntimeError(f"Failed to set credential in keychain: {e}")
        else:
            raise RuntimeError("Keychain storage only available on macOS")

    def delete_credential(self, key: str) -> None:
        """Delete credential from keychain.
        
        Args:
            key: Credential key name to delete
        """
        if self.is_macos:
            try:
                keyring.delete_password(self.service_name, key.lower())
            except Exception as e:
                raise RuntimeError(f"Failed to delete credential from keychain: {e}")
        else:
            raise RuntimeError("Keychain deletion only available on macOS")

    def get_snowflake_credentials(self) -> Dict[str, Any]:
        """Get Snowflake credentials.
        
        Returns:
            Dictionary with Snowflake connection parameters
        """
        # Get username from current system user (since only Snowflake needs username)
        username = get_current_user()
        
        # Get credentials from environment/keychain
        password = self.get_credential("SNOWFLAKE_PASSWORD")
        private_key_path = self.get_credential("SNOWFLAKE_PRIVATE_KEY_PATH")
        private_key_passphrase = self.get_credential("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        
        config = {
            "account": self.get_credential("SNOWFLAKE_ACCOUNT"),        # Required
            "user": username,                                           # Required (auto-detected)
            "warehouse": self.get_credential("SNOWFLAKE_WAREHOUSE"),    # Required
            "database": self.get_credential("SNOWFLAKE_DATABASE"),      # Optional
            "schema": self.get_credential("SNOWFLAKE_SCHEMA"),          # Optional
        }
        
        if password:
            config["password"] = password
        elif private_key_path:
            config["private_key_path"] = private_key_path
            if private_key_passphrase:
                config["private_key_passphrase"] = private_key_passphrase
        
        # SSO authentication
        authenticator = self.get_credential("SNOWFLAKE_AUTHENTICATOR")
        if authenticator:
            config["authenticator"] = authenticator
            
        return {k: v for k, v in config.items() if v is not None}

    def get_databricks_credentials(self) -> Dict[str, Any]:
        """Get Databricks credentials.
        
        Returns:
            Dictionary with Databricks connection parameters
        """
        return {
            "server_hostname": self.get_credential("DATABRICKS_SERVER_HOSTNAME"),
            "http_path": self.get_credential("DATABRICKS_HTTP_PATH"),
            "access_token": self.get_credential("DATABRICKS_ACCESS_TOKEN"),
            "catalog": self.get_credential("DATABRICKS_CATALOG"),
            "schema": self.get_credential("DATABRICKS_SCHEMA"),
        }

    def get_s3_credentials(self) -> Dict[str, Any]:
        """Get S3/AWS credentials.
        
        Returns:
            Dictionary with AWS/S3 connection parameters
        """
        return {
            "aws_access_key_id": self.get_credential("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": self.get_credential("AWS_SECRET_ACCESS_KEY"),
            "aws_session_token": self.get_credential("AWS_SESSION_TOKEN"),
            "region_name": self.get_credential("AWS_DEFAULT_REGION") or "us-east-1",
        }



    def set_snowflake_password(self, password: str) -> None:
        """Store Snowflake password in keychain.
        
        Args:
            password: Snowflake password
        """
        if not self.is_macos:
            raise RuntimeError("Keychain storage only available on macOS")
        
        # Store password in keychain
        self.set_credential("SNOWFLAKE_PASSWORD", password)
        
        print("✓ Stored Snowflake password in keychain")
        print(f"  Username will be auto-detected from system user: {get_current_user()}")

