"""Unit tests for SnowflakeConnector."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock

from cloudconduit.connectors.snowflake import SnowflakeConnector


class TestSnowflakeConnector:
    """Test SnowflakeConnector functionality."""

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    def test_init_with_username(self):
        """Test connector initialization with explicit username."""
        connector = SnowflakeConnector("test-user")
        assert connector.account == "test-account"
        assert connector.username == "test-user"
        assert connector._connection is None

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    @patch('cloudconduit.utils.config_manager.get_current_user')
    def test_init_default_username(self, mock_current_user):
        """Test connector initialization with default username."""
        mock_current_user.return_value = "system.user"
        
        connector = SnowflakeConnector()
        assert connector.account == "test-account"
        assert connector.username == "system.user"
        mock_current_user.assert_called_once()

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    def test_init_with_explicit_username(self):
        """Test connector initialization with explicitly provided username."""
        connector = SnowflakeConnector("explicit.user")
        assert connector.account == "test-account"
        assert connector.username == "explicit.user"

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    def test_init_with_config(self):
        """Test connector initialization with config."""
        config = {"warehouse": "test-wh", "database": "test-db"}
        connector = SnowflakeConnector("test-user", config)
        assert "warehouse" in connector.config
        assert "database" in connector.config

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    @patch('cloudconduit.connectors.snowflake.snowflake.connector.connect')
    def test_connect_success(self, mock_connect, mock_snowflake_connection):
        """Test successful connection."""
        mock_connect.return_value = mock_snowflake_connection
        
        connector = SnowflakeConnector("test-user")
        connector.connect()
        
        assert connector._connection is not None
        mock_connect.assert_called_once()

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    @patch('cloudconduit.connectors.snowflake.snowflake.connector.connect')
    def test_connect_failure(self, mock_connect):
        """Test connection failure."""
        mock_connect.side_effect = Exception("Connection failed")
        
        connector = SnowflakeConnector("test-user")
        
        with pytest.raises(ConnectionError, match="Failed to connect to Snowflake"):
            connector.connect()

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    def test_disconnect(self, mock_snowflake_connection):
        """Test disconnection."""
        connector = SnowflakeConnector("test-user")
        connector._connection = mock_snowflake_connection
        
        connector.disconnect()
        
        assert connector._connection is None
        mock_snowflake_connection.close.assert_called_once()

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    def test_disconnect_no_connection(self):
        """Test disconnection with no active connection."""
        connector = SnowflakeConnector("test-user")
        # Should not raise error
        connector.disconnect()

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    @patch('cloudconduit.connectors.snowflake.snowflake.connector.connect')
    def test_execute_select_query(self, mock_connect, mock_snowflake_connection):
        """Test executing SELECT query."""
        mock_connect.return_value = mock_snowflake_connection
        
        connector = SnowflakeConnector("test-user")
        result = connector.execute("SELECT * FROM test_table")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ['ID', 'NAME', 'VALUE']

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    @patch('cloudconduit.connectors.snowflake.snowflake.connector.connect')
    def test_execute_non_select_query(self, mock_connect, mock_snowflake_connection):
        """Test executing non-SELECT query."""
        mock_connect.return_value = mock_snowflake_connection
        mock_snowflake_connection.cursor().fetchall.return_value = []
        
        connector = SnowflakeConnector("test-user")
        result = connector.execute("INSERT INTO test_table VALUES (1, 'test')")
        
        assert result == []

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    @patch('cloudconduit.connectors.snowflake.snowflake.connector.connect')
    def test_execute_query_error(self, mock_connect, mock_snowflake_connection):
        """Test query execution error."""
        mock_connect.return_value = mock_snowflake_connection
        mock_snowflake_connection.cursor().execute.side_effect = Exception("SQL error")
        
        connector = SnowflakeConnector("test-user")
        
        with pytest.raises(RuntimeError, match="Query execution failed"):
            connector.execute("SELECT * FROM non_existent_table")

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    @patch('cloudconduit.connectors.snowflake.snowflake.connector.connect')
    @patch('cloudconduit.connectors.snowflake.write_pandas')
    def test_upload_df_success(self, mock_write_pandas, mock_connect, 
                              mock_snowflake_connection, sample_dataframe):
        """Test successful DataFrame upload."""
        mock_connect.return_value = mock_snowflake_connection
        mock_write_pandas.return_value = (True, 1, 5, None)
        
        connector = SnowflakeConnector("test-user")
        connector.upload_df(sample_dataframe, "test_table")
        
        mock_write_pandas.assert_called_once()
        args, kwargs = mock_write_pandas.call_args
        assert args[1].equals(sample_dataframe)
        assert args[2] == "TEST_TABLE"  # Should be uppercase
        assert kwargs['auto_create_table'] is True

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    @patch('cloudconduit.connectors.snowflake.snowflake.connector.connect')
    @patch('cloudconduit.connectors.snowflake.write_pandas')
    def test_upload_df_failure(self, mock_write_pandas, mock_connect, 
                              mock_snowflake_connection, sample_dataframe):
        """Test DataFrame upload failure."""
        mock_connect.return_value = mock_snowflake_connection
        mock_write_pandas.return_value = (False, 0, 0, None)
        
        connector = SnowflakeConnector("test-user")
        
        with pytest.raises(RuntimeError, match="Failed to upload DataFrame"):
            connector.upload_df(sample_dataframe, "test_table")

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    @patch('cloudconduit.connectors.snowflake.snowflake.connector.connect')
    def test_copy_table_replace(self, mock_connect, mock_snowflake_connection):
        """Test table copy with replace."""
        mock_connect.return_value = mock_snowflake_connection
        
        connector = SnowflakeConnector("test-user")
        connector.copy_table("source_table", "target_table", if_exists="replace")
        
        # Should execute DROP and CREATE
        cursor = mock_snowflake_connection.cursor()
        assert cursor.execute.call_count >= 2

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    @patch('cloudconduit.connectors.snowflake.snowflake.connector.connect')
    def test_copy_table_fail_exists(self, mock_connect, mock_snowflake_connection):
        """Test table copy fails when target exists."""
        mock_connect.return_value = mock_snowflake_connection
        
        # Mock table exists check
        mock_cursor = mock_snowflake_connection.cursor()
        mock_cursor.fetchall.return_value = [(1,)]  # Table exists
        mock_cursor.description = [('cnt', 'NUMBER')]
        
        connector = SnowflakeConnector("test-user")
        
        with pytest.raises(RuntimeError, match="already exists"):
            connector.copy_table("source_table", "target_table", if_exists="fail")

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    @patch('cloudconduit.connectors.snowflake.snowflake.connector.connect')
    def test_grant_access_to_role(self, mock_connect, mock_snowflake_connection):
        """Test granting access to role."""
        mock_connect.return_value = mock_snowflake_connection
        
        connector = SnowflakeConnector("test-user")
        connector.grant_access("test_table", "ROLE_DATA_READER", "SELECT")
        
        cursor = mock_snowflake_connection.cursor()
        cursor.execute.assert_called()
        call_args = cursor.execute.call_args[0][0]
        assert "ROLE ROLE_DATA_READER" in call_args

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    @patch('cloudconduit.connectors.snowflake.snowflake.connector.connect')
    def test_grant_access_to_user(self, mock_connect, mock_snowflake_connection):
        """Test granting access to user."""
        mock_connect.return_value = mock_snowflake_connection
        
        connector = SnowflakeConnector("test-user")
        connector.grant_access("test_table", "john.doe", "SELECT")
        
        cursor = mock_snowflake_connection.cursor()
        cursor.execute.assert_called()
        call_args = cursor.execute.call_args[0][0]
        assert "USER john.doe" in call_args

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    @patch('cloudconduit.connectors.snowflake.snowflake.connector.connect')
    def test_list_tables(self, mock_connect, mock_snowflake_connection):
        """Test listing tables."""
        mock_connect.return_value = mock_snowflake_connection
        mock_cursor = mock_snowflake_connection.cursor()
        mock_cursor.description = [('TABLE_CATALOG', 'VARCHAR'), 
                                  ('TABLE_SCHEMA', 'VARCHAR'),
                                  ('TABLE_NAME', 'VARCHAR'),
                                  ('TABLE_TYPE', 'VARCHAR')]
        mock_cursor.fetchall.return_value = [
            ('DB', 'SCHEMA', 'TABLE1', 'BASE TABLE'),
            ('DB', 'SCHEMA', 'TABLE2', 'BASE TABLE')
        ]
        
        connector = SnowflakeConnector("test-user")
        result = connector.list_tables()
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    @patch('cloudconduit.connectors.snowflake.snowflake.connector.connect')
    def test_get_table_info(self, mock_connect, mock_snowflake_connection):
        """Test getting table information."""
        mock_connect.return_value = mock_snowflake_connection
        mock_cursor = mock_snowflake_connection.cursor()
        mock_cursor.description = [('COLUMN_NAME', 'VARCHAR'),
                                  ('DATA_TYPE', 'VARCHAR'),
                                  ('IS_NULLABLE', 'VARCHAR'),
                                  ('COLUMN_DEFAULT', 'VARCHAR')]
        mock_cursor.fetchall.return_value = [
            ('ID', 'NUMBER', 'NO', None),
            ('NAME', 'VARCHAR', 'YES', None)
        ]
        
        connector = SnowflakeConnector("test-user")
        result = connector.get_table_info("test_table")
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    def test_context_manager(self, mock_snowflake_connection):
        """Test context manager functionality."""
        with patch('cloudconduit.connectors.snowflake.snowflake.connector.connect') as mock_connect:
            mock_connect.return_value = mock_snowflake_connection
            
            with SnowflakeConnector("test-user") as connector:
                assert connector._connection is not None
            
            # Should disconnect on exit
            mock_snowflake_connection.close.assert_called_once()

    @patch.dict('os.environ', {'SNOWFLAKE_ACCOUNT': 'test-account', 'SNOWFLAKE_WAREHOUSE': 'test-wh'})
    def test_is_connected_property(self):
        """Test is_connected property."""
        connector = SnowflakeConnector("test-user")
        assert connector.is_connected is False
        
        connector._connection = Mock()
        assert connector.is_connected is True