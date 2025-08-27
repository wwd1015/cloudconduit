"""Base connector class for all cloud service connections."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import pandas as pd


class BaseConnector(ABC):
    """Abstract base class for all cloud service connectors."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the connector with configuration.
        
        Args:
            config: Configuration dictionary for the connector
        """
        self.config = config or {}
        self._connection = None

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the service."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection to the service."""
        pass

    @abstractmethod
    def execute(self, query: str, **kwargs) -> Any:
        """Execute a query or command.
        
        Args:
            query: SQL query or command to execute
            **kwargs: Additional parameters
            
        Returns:
            Query results
        """
        pass

    @abstractmethod
    def upload_df(self, df: pd.DataFrame, table_name: str, **kwargs) -> None:
        """Upload a pandas DataFrame to a table.
        
        Args:
            df: DataFrame to upload
            table_name: Target table name
            **kwargs: Additional parameters
        """
        pass

    def copy_table(self, source_table: str, target_table: str, **kwargs) -> None:
        """Copy a table.
        
        Args:
            source_table: Source table name
            target_table: Target table name
            **kwargs: Additional parameters
        """
        query = f"CREATE TABLE {target_table} AS SELECT * FROM {source_table}"
        self.execute(query, **kwargs)

    def drop_table(self, table_name: str, **kwargs) -> None:
        """Drop a table.
        
        Args:
            table_name: Table name to drop
            **kwargs: Additional parameters
        """
        query = f"DROP TABLE IF EXISTS {table_name}"
        self.execute(query, **kwargs)

    def grant_access(self, table_name: str, user_or_role: str, privileges: str = "SELECT", **kwargs) -> None:
        """Grant access to a table.
        
        Args:
            table_name: Table name
            user_or_role: User or role to grant access to
            privileges: Privileges to grant (default: SELECT)
            **kwargs: Additional parameters
        """
        query = f"GRANT {privileges} ON {table_name} TO {user_or_role}"
        self.execute(query, **kwargs)

    @property
    def is_connected(self) -> bool:
        """Check if connection is active."""
        return self._connection is not None

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()