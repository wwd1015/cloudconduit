"""Snowflake connector with SSO and keychain support."""

from typing import Any, Dict, Optional, Union
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from .base import BaseConnector
from ..utils.config_manager import ConfigManager


class SnowflakeConnector(BaseConnector):
    """Snowflake database connector with credential management."""

    def __init__(self, username: Optional[str] = None, config: Optional[Dict[str, Any]] = None):
        """Initialize Snowflake connector.
        
        Args:
            username: Snowflake username. If None, uses current system user as default.
            config: Additional configuration parameters (highest priority)
        """
        super().__init__(config)
        
        # Initialize configuration manager
        self.config_manager = ConfigManager()
        
        # Get complete configuration following priority order
        self.config = self.config_manager.get_snowflake_config(username, config)
        
        # Extract and validate required parameters
        self.account = self.config.get("account")
        self.warehouse = self.config.get("warehouse")
        self.username = self.config.get("user")
        
        if not self.account:
            raise ValueError("Snowflake account not found. Please set SNOWFLAKE_ACCOUNT environment variable or update config.yaml")
        
        if not self.warehouse:
            raise ValueError("Snowflake warehouse not found. Please set SNOWFLAKE_WAREHOUSE environment variable or update config.yaml")

    def connect(self) -> None:
        """Establish connection to Snowflake."""
        if self._connection:
            return
            
        try:
            connection_params = {
                "account": self.config.get("account", self.account),
                "user": self.config.get("user", self.username),
            }
            
            # Add optional parameters if present
            optional_params = [
                "password", "private_key_path", "private_key_passphrase",
                "authenticator", "warehouse", "database", "schema", "role"
            ]
            
            for param in optional_params:
                if param in self.config:
                    connection_params[param] = self.config[param]
            
            self._connection = snowflake.connector.connect(**connection_params)
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Snowflake: {e}")

    def disconnect(self) -> None:
        """Close connection to Snowflake."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def execute(self, query: str, **kwargs) -> Union[pd.DataFrame, Any]:
        """Execute SQL query on Snowflake.
        
        Args:
            query: SQL query to execute
            **kwargs: Additional parameters
            
        Returns:
            DataFrame for SELECT queries, cursor result for others
        """
        if not self._connection:
            self.connect()
            
        cursor = self._connection.cursor()
        
        try:
            cursor.execute(query)
            
            # For SELECT queries, return as DataFrame
            if query.strip().upper().startswith('SELECT'):
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=columns)
            
            # For other queries, return cursor result
            return cursor.fetchall()
            
        except Exception as e:
            raise RuntimeError(f"Query execution failed: {e}")
        finally:
            cursor.close()

    def upload_df(self, df: pd.DataFrame, table_name: str, if_exists: str = "replace", **kwargs) -> None:
        """Upload pandas DataFrame to Snowflake table.
        
        Args:
            df: DataFrame to upload
            table_name: Target table name
            if_exists: What to do if table exists ('replace', 'append', 'fail')
            **kwargs: Additional parameters
        """
        if not self._connection:
            self.connect()
            
        try:
            success, nchunks, nrows, _ = write_pandas(
                self._connection,
                df,
                table_name.upper(),
                auto_create_table=True,
                overwrite=(if_exists == "replace"),
                **kwargs
            )
            
            if not success:
                raise RuntimeError(f"Failed to upload DataFrame to {table_name}")
                
        except Exception as e:
            raise RuntimeError(f"DataFrame upload failed: {e}")

    def copy_table(self, source_table: str, target_table: str, if_exists: str = "replace", **kwargs) -> None:
        """Copy a Snowflake table.
        
        Args:
            source_table: Source table name
            target_table: Target table name  
            if_exists: What to do if target exists ('replace', 'fail')
            **kwargs: Additional parameters
        """
        if if_exists == "replace":
            self.drop_table(target_table)
        elif if_exists == "fail":
            # Check if table exists
            check_query = f"""
            SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '{target_table.upper()}'
            """
            result = self.execute(check_query)
            if result.iloc[0]['cnt'] > 0:
                raise RuntimeError(f"Table {target_table} already exists")
        
        query = f"CREATE TABLE {target_table} AS SELECT * FROM {source_table}"
        self.execute(query, **kwargs)

    def grant_access(self, table_name: str, user_or_role: str, privileges: str = "SELECT", **kwargs) -> None:
        """Grant access to a Snowflake table.
        
        Args:
            table_name: Table name
            user_or_role: User or role to grant access to
            privileges: Privileges to grant (default: SELECT)
            **kwargs: Additional parameters
        """
        # Determine if it's a role or user
        if user_or_role.upper().startswith('ROLE_') or 'ROLE' in kwargs:
            grant_to = f"ROLE {user_or_role}"
        else:
            grant_to = f"USER {user_or_role}"
            
        query = f"GRANT {privileges} ON TABLE {table_name} TO {grant_to}"
        self.execute(query, **kwargs)

    def list_tables(self, schema: Optional[str] = None) -> pd.DataFrame:
        """List tables in the current or specified schema.
        
        Args:
            schema: Schema name (optional)
            
        Returns:
            DataFrame with table information
        """
        schema_filter = f"AND TABLE_SCHEMA = '{schema.upper()}'" if schema else ""
        
        query = f"""
        SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_CATALOG = CURRENT_DATABASE()
        {schema_filter}
        ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        
        return self.execute(query)

    def get_table_info(self, table_name: str, schema: Optional[str] = None) -> pd.DataFrame:
        """Get column information for a table.
        
        Args:
            table_name: Table name
            schema: Schema name (optional)
            
        Returns:
            DataFrame with column information
        """
        schema_filter = f"AND TABLE_SCHEMA = '{schema.upper()}'" if schema else ""
        
        query = f"""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name.upper()}'
        AND TABLE_CATALOG = CURRENT_DATABASE()
        {schema_filter}
        ORDER BY ORDINAL_POSITION
        """
        
        return self.execute(query)