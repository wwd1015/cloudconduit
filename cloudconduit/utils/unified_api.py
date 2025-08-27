"""Unified API functions for easy access to all connectors."""

from typing import Any, Dict, Optional, Union
import pandas as pd

from ..connectors.snowflake import SnowflakeConnector
from ..connectors.databricks import DatabricksConnector
from ..connectors.s3 import S3Connector


class CloudConduit:
    """Unified interface for all cloud connectors."""

    def __init__(self):
        """Initialize CloudConduit with empty connectors."""
        self._snowflake = None
        self._databricks = None
        self._s3 = None

    def snowflake(self, username: Optional[str] = None, config: Optional[Dict[str, Any]] = None) -> SnowflakeConnector:
        """Get or create Snowflake connector.
        
        Args:
            username: Snowflake username. If None, uses current system user as default.
            config: Additional configuration
            
        Returns:
            SnowflakeConnector instance
        """
        if not self._snowflake:
            self._snowflake = SnowflakeConnector(username, config)
        return self._snowflake

    def databricks(self, config: Optional[Dict[str, Any]] = None) -> DatabricksConnector:
        """Get or create Databricks connector.
        
        Args:
            config: Additional configuration
            
        Returns:
            DatabricksConnector instance
        """
        if not self._databricks:
            self._databricks = DatabricksConnector(config)
        return self._databricks

    def s3(self, config: Optional[Dict[str, Any]] = None) -> S3Connector:
        """Get or create S3 connector.
        
        Args:
            config: Additional configuration
            
        Returns:
            S3Connector instance
        """
        if not self._s3:
            self._s3 = S3Connector(config)
        return self._s3


# Convenience functions for common operations
def connect_snowflake(username: Optional[str] = None, config: Optional[Dict[str, Any]] = None) -> SnowflakeConnector:
    """Create and connect to Snowflake.
    
    Args:
        username: Snowflake username. If None, uses current system user as default.
        config: Additional configuration
        
    Returns:
        Connected SnowflakeConnector
    """
    connector = SnowflakeConnector(username, config)
    connector.connect()
    return connector


def connect_databricks(config: Optional[Dict[str, Any]] = None) -> DatabricksConnector:
    """Create and connect to Databricks.
    
    Args:
        config: Additional configuration
        
    Returns:
        Connected DatabricksConnector
    """
    connector = DatabricksConnector(config)
    connector.connect()
    return connector


def connect_s3(config: Optional[Dict[str, Any]] = None) -> S3Connector:
    """Create and connect to S3.
    
    Args:
        config: Additional configuration
        
    Returns:
        Connected S3Connector
    """
    connector = S3Connector(config)
    connector.connect()
    return connector


def execute(connector: Union[SnowflakeConnector, DatabricksConnector], 
           query: str, **kwargs) -> Union[pd.DataFrame, Any]:
    """Execute query on database connector.
    
    Args:
        connector: Database connector (Snowflake or Databricks)
        query: SQL query to execute
        **kwargs: Additional parameters
        
    Returns:
        Query results
    """
    return connector.execute(query, **kwargs)


def upload_df(connector: Union[SnowflakeConnector, DatabricksConnector, S3Connector],
             df: pd.DataFrame, destination: str, **kwargs) -> None:
    """Upload DataFrame to connector destination.
    
    Args:
        connector: Any connector
        df: DataFrame to upload
        destination: Destination (table name for DB, bucket/key for S3)
        **kwargs: Additional parameters
    """
    if isinstance(connector, S3Connector):
        # For S3, destination should be in format "bucket/key"
        if "/" not in destination:
            raise ValueError("S3 destination must be in format 'bucket/key'")
        bucket, key = destination.split("/", 1)
        connector.upload_df(df, bucket, key, **kwargs)
    else:
        # For databases, destination is table name
        connector.upload_df(df, destination, **kwargs)




def copy_table(connector: Union[SnowflakeConnector, DatabricksConnector, S3Connector],
              source: str, target: str, **kwargs) -> None:
    """Copy table or object.
    
    Args:
        connector: Any connector
        source: Source table/object
        target: Target table/object
        **kwargs: Additional parameters
    """
    connector.copy_table(source, target, **kwargs)


def drop_table(connector: Union[SnowflakeConnector, DatabricksConnector, S3Connector],
              table_name: str, **kwargs) -> None:
    """Drop table or object.
    
    Args:
        connector: Any connector
        table_name: Table/object name to drop
        **kwargs: Additional parameters
    """
    connector.drop_table(table_name, **kwargs)


def grant_access(connector: Union[SnowflakeConnector, DatabricksConnector, S3Connector],
                table_name: str, user_or_role: str, privileges: str = "SELECT", **kwargs) -> None:
    """Grant access to table or object.
    
    Args:
        connector: Any connector
        table_name: Table/object name
        user_or_role: User or role to grant access to
        privileges: Privileges to grant
        **kwargs: Additional parameters
    """
    connector.grant_access(table_name, user_or_role, privileges, **kwargs)