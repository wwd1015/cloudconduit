"""CloudConduit - Unified API for Snowflake, Databricks, and S3."""

from .connectors.snowflake import SnowflakeConnector
from .connectors.databricks import DatabricksConnector
from .connectors.s3 import S3Connector
from .utils.config_manager import ConfigManager
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

__version__ = "0.2.0"
__all__ = [
    "SnowflakeConnector",
    "DatabricksConnector", 
    "S3Connector",
    "ConfigManager",
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