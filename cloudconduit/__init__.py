"""CloudConduit - Unified API for Snowflake, Databricks, and S3."""

import os
from .connectors.snowflake import SnowflakeConnector
from .connectors.databricks import DatabricksConnector
from .connectors.s3 import S3Connector
from .utils.credential_manager import CredentialManager
from .utils.config_helper import push_config_to_env, show_config, create_user_config

# Automatically load config on package import (unless disabled)
def _auto_load_config():
    """Auto-load configuration if not disabled."""
    if not os.getenv("CLOUDCONDUIT_DISABLE_AUTO_CONFIG"):
        try:
            push_config_to_env()
        except Exception:
            # Silently ignore config loading errors to avoid breaking imports
            pass

# Only auto-load if importing the full package (not individual modules)
import sys
if __name__ in sys.modules and not getattr(sys.modules[__name__], '_config_loaded', False):
    _auto_load_config()
    sys.modules[__name__]._config_loaded = True
from .utils.unified_api import (
    CloudConduit,
    connect_snowflake,
    connect_databricks,
    connect_s3,
    execute,
    upload_df,
    copy_table,
    drop_table,
    grant_access,
)

__version__ = "0.1.0"
__all__ = [
    "SnowflakeConnector",
    "DatabricksConnector", 
    "S3Connector",
    "CredentialManager",
    "push_config_to_env",
    "show_config",
    "create_user_config",
    "CloudConduit",
    "connect_snowflake",
    "connect_databricks", 
    "connect_s3",
    "execute",
    "upload_df",
    "copy_table",
    "drop_table",
    "grant_access",
]