"""Unit tests for DatabricksConnector."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch

from cloudconduit.connectors.databricks import DatabricksConnector


class TestDatabricksConnector:
    """Test DatabricksConnector functionality."""

    def test_init(self):
        """Test connector initialization."""
        connector = DatabricksConnector()
        assert connector._connection is None

    def test_init_with_config(self):
        """Test connector initialization with config."""
        config = {"catalog": "test-catalog", "schema": "test-schema"}
        connector = DatabricksConnector(config)
        assert "catalog" in connector.config
        assert "schema" in connector.config

    @patch('cloudconduit.connectors.databricks.databricks_sql.connect')
    def test_connect_success(self, mock_connect, mock_databricks_connection, mock_environment):
        """Test successful connection."""
        mock_connect.return_value = mock_databricks_connection
        
        connector = DatabricksConnector()
        connector.connect()
        
        assert connector._connection is not None
        mock_connect.assert_called_once_with(
            server_hostname="test-workspace.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test-warehouse",
            access_token="test-token"
        )

    @patch('cloudconduit.connectors.databricks.databricks_sql.connect')
    def test_connect_missing_params(self, mock_connect):
        """Test connection with missing parameters."""
        connector = DatabricksConnector()
        
        with pytest.raises(ConnectionError, match="Missing required parameters"):
            connector.connect()

    @patch('cloudconduit.connectors.databricks.databricks_sql.connect')
    def test_connect_failure(self, mock_connect, mock_environment):
        """Test connection failure."""
        mock_connect.side_effect = Exception("Connection failed")
        
        connector = DatabricksConnector()
        
        with pytest.raises(ConnectionError, match="Failed to connect to Databricks"):
            connector.connect()

    def test_disconnect(self, mock_databricks_connection):
        """Test disconnection."""
        connector = DatabricksConnector()
        connector._connection = mock_databricks_connection
        
        connector.disconnect()
        
        assert connector._connection is None
        mock_databricks_connection.close.assert_called_once()

    @patch('cloudconduit.connectors.databricks.databricks_sql.connect')
    def test_execute_select_query(self, mock_connect, mock_databricks_connection, mock_environment):
        """Test executing SELECT query."""
        mock_connect.return_value = mock_databricks_connection
        
        connector = DatabricksConnector()
        result = connector.execute("SELECT * FROM test_table")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch('cloudconduit.connectors.databricks.databricks_sql.connect')
    def test_execute_non_select_query(self, mock_connect, mock_databricks_connection, mock_environment):
        """Test executing non-SELECT query."""
        mock_connect.return_value = mock_databricks_connection
        mock_databricks_connection.cursor().fetchall.return_value = []
        
        connector = DatabricksConnector()
        result = connector.execute("CREATE TABLE test AS SELECT * FROM source")
        
        assert result == []

    @patch('cloudconduit.connectors.databricks.databricks_sql.connect')
    def test_execute_query_error(self, mock_connect, mock_databricks_connection, mock_environment):
        """Test query execution error."""
        mock_connect.return_value = mock_databricks_connection
        mock_databricks_connection.cursor().execute.side_effect = Exception("SQL error")
        
        connector = DatabricksConnector()
        
        with pytest.raises(RuntimeError, match="Query execution failed"):
            connector.execute("SELECT * FROM non_existent_table")

    @patch('cloudconduit.connectors.databricks.databricks_sql.connect')
    def test_upload_df_simple_table_name(self, mock_connect, mock_databricks_connection, 
                                        sample_dataframe, mock_environment):
        """Test DataFrame upload with simple table name."""
        mock_connect.return_value = mock_databricks_connection
        
        connector = DatabricksConnector()
        connector.upload_df(sample_dataframe, "test_table")
        
        # Should execute CREATE TABLE and INSERT statements
        cursor = mock_databricks_connection.cursor()
        assert cursor.execute.call_count >= 2  # CREATE + INSERT(s)

    @patch('cloudconduit.connectors.databricks.databricks_sql.connect')
    def test_upload_df_full_table_name(self, mock_connect, mock_databricks_connection, 
                                      sample_dataframe, mock_environment):
        """Test DataFrame upload with full table name."""
        mock_connect.return_value = mock_databricks_connection
        
        connector = DatabricksConnector()
        connector.upload_df(sample_dataframe, "catalog.schema.table")
        
        cursor = mock_databricks_connection.cursor()
        # Check that CREATE TABLE uses full name
        create_call = cursor.execute.call_args_list[0][0][0]
        assert "catalog.schema.table" in create_call

    @patch('cloudconduit.connectors.databricks.databricks_sql.connect')
    def test_upload_df_if_exists_fail(self, mock_connect, mock_databricks_connection, 
                                     sample_dataframe, mock_environment):
        """Test DataFrame upload with if_exists='fail' when table exists."""
        mock_connect.return_value = mock_databricks_connection
        
        # Mock table exists
        mock_cursor = mock_databricks_connection.cursor()
        mock_cursor.fetchall.return_value = [("test_table", "default")]
        mock_cursor.description = [('table_name', 'STRING'), ('database', 'STRING')]
        
        connector = DatabricksConnector()
        
        with pytest.raises(RuntimeError, match="already exists"):
            connector.upload_df(sample_dataframe, "test_table", if_exists="fail")

    @patch('cloudconduit.connectors.databricks.databricks_sql.connect')
    def test_copy_table(self, mock_connect, mock_databricks_connection, mock_environment):
        """Test table copy."""
        mock_connect.return_value = mock_databricks_connection
        
        connector = DatabricksConnector()
        connector.copy_table("source_table", "target_table")
        
        cursor = mock_databricks_connection.cursor()
        cursor.execute.assert_called()

    @patch('cloudconduit.connectors.databricks.databricks_sql.connect')
    def test_grant_access(self, mock_connect, mock_databricks_connection, mock_environment):
        """Test granting access."""
        mock_connect.return_value = mock_databricks_connection
        
        connector = DatabricksConnector()
        connector.grant_access("test_table", "user@example.com", "SELECT")
        
        cursor = mock_databricks_connection.cursor()
        cursor.execute.assert_called()
        call_args = cursor.execute.call_args[0][0]
        assert "GRANT SELECT" in call_args
        assert "`user@example.com`" in call_args

    @patch('cloudconduit.connectors.databricks.databricks_sql.connect')
    def test_list_tables(self, mock_connect, mock_databricks_connection, mock_environment):
        """Test listing tables."""
        mock_connect.return_value = mock_databricks_connection
        
        connector = DatabricksConnector()
        result = connector.list_tables()
        
        assert isinstance(result, pd.DataFrame)
        cursor = mock_databricks_connection.cursor()
        cursor.execute.assert_called()

    @patch('cloudconduit.connectors.databricks.databricks_sql.connect')
    def test_list_tables_with_params(self, mock_connect, mock_databricks_connection, mock_environment):
        """Test listing tables with catalog and schema."""
        mock_connect.return_value = mock_databricks_connection
        
        connector = DatabricksConnector()
        result = connector.list_tables("custom_catalog", "custom_schema")
        
        cursor = mock_databricks_connection.cursor()
        call_args = cursor.execute.call_args[0][0]
        assert "custom_catalog.custom_schema" in call_args

    @patch('cloudconduit.connectors.databricks.databricks_sql.connect')
    def test_list_schemas(self, mock_connect, mock_databricks_connection, mock_environment):
        """Test listing schemas."""
        mock_connect.return_value = mock_databricks_connection
        
        connector = DatabricksConnector()
        result = connector.list_schemas()
        
        assert isinstance(result, pd.DataFrame)
        cursor = mock_databricks_connection.cursor()
        cursor.execute.assert_called()

    @patch('cloudconduit.connectors.databricks.databricks_sql.connect')
    def test_get_table_info(self, mock_connect, mock_databricks_connection, mock_environment):
        """Test getting table information."""
        mock_connect.return_value = mock_databricks_connection
        
        connector = DatabricksConnector()
        result = connector.get_table_info("test_table")
        
        assert isinstance(result, pd.DataFrame)
        cursor = mock_databricks_connection.cursor()
        cursor.execute.assert_called()

    def test_context_manager(self, mock_databricks_connection):
        """Test context manager functionality."""
        with patch('cloudconduit.connectors.databricks.databricks_sql.connect') as mock_connect:
            mock_connect.return_value = mock_databricks_connection
            
            with patch.dict('os.environ', {
                'DATABRICKS_SERVER_HOSTNAME': 'test',
                'DATABRICKS_HTTP_PATH': 'test',
                'DATABRICKS_ACCESS_TOKEN': 'test'
            }):
                with DatabricksConnector() as connector:
                    assert connector._connection is not None
                
                mock_databricks_connection.close.assert_called_once()