"""Databricks connector with credential management."""

from typing import Any, Dict, Optional, Union
import pandas as pd
from databricks import sql as databricks_sql

from .base import BaseConnector
from ..utils.config_manager import ConfigManager


class DatabricksConnector(BaseConnector):
    """Databricks SQL connector with credential management."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize Databricks connector.
        
        Args:
            config: Additional configuration parameters (highest priority)
        """
        super().__init__(config)
        
        # Initialize configuration manager
        self.config_manager = ConfigManager()
        
        # Get complete configuration following priority order
        self.config = self.config_manager.get_databricks_config(config)

    def connect(self) -> None:
        """Establish connection to Databricks."""
        if self._connection:
            return
            
        try:
            required_params = ["server_hostname", "http_path", "access_token"]
            missing_params = [p for p in required_params if not self.config.get(p)]
            
            if missing_params:
                raise ValueError(f"Missing required parameters: {missing_params}")
            
            self._connection = databricks_sql.connect(
                server_hostname=self.config["server_hostname"],
                http_path=self.config["http_path"],
                access_token=self.config["access_token"]
            )
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Databricks: {e}")

    def disconnect(self) -> None:
        """Close connection to Databricks."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def execute(self, query: str, **kwargs) -> Union[pd.DataFrame, Any]:
        """Execute SQL query on Databricks.
        
        Args:
            query: SQL query to execute
            **kwargs: Additional parameters
            
        Returns:
            DataFrame for SELECT queries, result for others
        """
        if not self._connection:
            self.connect()
            
        cursor = self._connection.cursor()
        
        try:
            cursor.execute(query)
            
            # For SELECT, SHOW, and DESCRIBE queries, return as DataFrame
            if query.strip().upper().startswith(('SELECT', 'SHOW', 'DESCRIBE')):
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=columns)
            
            # For other queries, return result
            result = cursor.fetchall()
            return result
            
        except Exception as e:
            raise RuntimeError(f"Query execution failed: {e}")
        finally:
            cursor.close()

    def upload_df(self, df: pd.DataFrame, table_name: str, if_exists: str = "replace", **kwargs) -> None:
        """Upload pandas DataFrame to Databricks table.
        
        Args:
            df: DataFrame to upload
            table_name: Target table name (format: catalog.schema.table)
            if_exists: What to do if table exists ('replace', 'append', 'fail')
            **kwargs: Additional parameters
        """
        if not self._connection:
            self.connect()

        # Parse table name
        catalog = self.config.get("catalog", "main")
        schema = self.config.get("schema", "default")
        
        if "." in table_name:
            parts = table_name.split(".")
            if len(parts) == 3:
                catalog, schema, table_name = parts
            elif len(parts) == 2:
                schema, table_name = parts
        
        full_table_name = f"{catalog}.{schema}.{table_name}"
        
        try:
            # Handle if_exists logic
            if if_exists == "replace":
                self.drop_table(full_table_name)
            elif if_exists == "fail":
                # Check if table exists
                check_query = f"SHOW TABLES IN {catalog}.{schema} LIKE '{table_name}'"
                result = self.execute(check_query)
                if not result.empty:
                    raise RuntimeError(f"Table {full_table_name} already exists")
            
            # Create table from DataFrame
            cursor = self._connection.cursor()
            
            # Get column definitions from DataFrame
            col_defs = []
            for col, dtype in df.dtypes.items():
                if pd.api.types.is_integer_dtype(dtype):
                    sql_type = "BIGINT"
                elif pd.api.types.is_float_dtype(dtype):
                    sql_type = "DOUBLE"
                elif pd.api.types.is_bool_dtype(dtype):
                    sql_type = "BOOLEAN"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    sql_type = "TIMESTAMP"
                else:
                    sql_type = "STRING"
                col_defs.append(f"`{col}` {sql_type}")
            
            # Create table
            create_query = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                {', '.join(col_defs)}
            )
            """
            cursor.execute(create_query)
            
            # Insert data in batches
            batch_size = kwargs.get('batch_size', 1000)
            for start_idx in range(0, len(df), batch_size):
                end_idx = min(start_idx + batch_size, len(df))
                batch_df = df.iloc[start_idx:end_idx]
                
                # Create VALUES clause
                values_list = []
                for _, row in batch_df.iterrows():
                    row_values = []
                    for val in row:
                        if pd.isna(val):
                            row_values.append("NULL")
                        elif isinstance(val, str):
                            row_values.append(f"'{val.replace("'", "''")}'")
                        else:
                            row_values.append(str(val))
                    values_list.append(f"({', '.join(row_values)})")
                
                if values_list:
                    insert_query = f"""
                    INSERT INTO {full_table_name} 
                    VALUES {', '.join(values_list)}
                    """
                    cursor.execute(insert_query)
            
            cursor.close()
            
        except Exception as e:
            raise RuntimeError(f"DataFrame upload failed: {e}")

    def copy_table(self, source_table: str, target_table: str, if_exists: str = "replace", **kwargs) -> None:
        """Copy a Databricks table.
        
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
            parts = target_table.split(".")
            if len(parts) >= 2:
                catalog_schema = ".".join(parts[:-1])
                table_name = parts[-1]
                check_query = f"SHOW TABLES IN {catalog_schema} LIKE '{table_name}'"
                result = self.execute(check_query)
                if not result.empty:
                    raise RuntimeError(f"Table {target_table} already exists")
        
        query = f"CREATE TABLE {target_table} AS SELECT * FROM {source_table}"
        self.execute(query, **kwargs)

    def grant_access(self, table_name: str, user_or_role: str, privileges: str = "SELECT", **kwargs) -> None:
        """Grant access to a Databricks table.
        
        Args:
            table_name: Table name
            user_or_role: User or service principal to grant access to
            privileges: Privileges to grant (default: SELECT)
            **kwargs: Additional parameters
        """
        query = f"GRANT {privileges} ON TABLE {table_name} TO `{user_or_role}`"
        self.execute(query, **kwargs)

    def list_tables(self, catalog: Optional[str] = None, schema: Optional[str] = None) -> pd.DataFrame:
        """List tables in the specified catalog and schema.
        
        Args:
            catalog: Catalog name (optional, uses config default)
            schema: Schema name (optional, uses config default)
            
        Returns:
            DataFrame with table information
        """
        catalog = catalog or self.config.get("catalog", "main")
        schema = schema or self.config.get("schema", "default")
        
        query = f"SHOW TABLES IN {catalog}.{schema}"
        return self.execute(query)

    def list_schemas(self, catalog: Optional[str] = None) -> pd.DataFrame:
        """List schemas in the specified catalog.
        
        Args:
            catalog: Catalog name (optional, uses config default)
            
        Returns:
            DataFrame with schema information
        """
        catalog = catalog or self.config.get("catalog", "main")
        
        query = f"SHOW SCHEMAS IN {catalog}"
        return self.execute(query)

    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """Get column information for a table.
        
        Args:
            table_name: Table name
            
        Returns:
            DataFrame with column information
        """
        query = f"DESCRIBE {table_name}"
        return self.execute(query)