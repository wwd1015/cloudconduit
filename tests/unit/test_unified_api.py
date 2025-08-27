"""Unit tests for unified API functions."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch

from cloudconduit.utils.unified_api import (
    CloudConduit,
    connect_snowflake,
    connect_databricks,
    connect_s3,
    execute,
    upload_df,
    copy_table,
    drop_table,
    grant_access
)
from cloudconduit.connectors.snowflake import SnowflakeConnector
from cloudconduit.connectors.databricks import DatabricksConnector
from cloudconduit.connectors.s3 import S3Connector


class TestCloudConduit:
    """Test CloudConduit unified interface."""

    def test_init(self):
        """Test CloudConduit initialization."""
        cc = CloudConduit()
        assert cc._snowflake is None
        assert cc._databricks is None
        assert cc._s3 is None

    @patch('cloudconduit.utils.unified_api.SnowflakeConnector')
    def test_snowflake_connector_creation_with_username(self, mock_sf_class):
        """Test Snowflake connector creation with explicit username."""
        mock_connector = Mock()
        mock_sf_class.return_value = mock_connector
        
        cc = CloudConduit()
        sf = cc.snowflake("test-user")
        
        assert sf is mock_connector
        assert cc._snowflake is mock_connector
        mock_sf_class.assert_called_once_with("test-user", None)

    @patch('cloudconduit.utils.unified_api.SnowflakeConnector')
    def test_snowflake_connector_creation_default_username(self, mock_sf_class):
        """Test Snowflake connector creation with default username."""
        mock_connector = Mock()
        mock_sf_class.return_value = mock_connector
        
        cc = CloudConduit()
        sf = cc.snowflake()
        
        assert sf is mock_connector
        assert cc._snowflake is mock_connector
        mock_sf_class.assert_called_once_with(None, None)

    @patch('cloudconduit.utils.unified_api.SnowflakeConnector')
    def test_snowflake_connector_reuse(self, mock_sf_class):
        """Test Snowflake connector reuse."""
        mock_connector = Mock()
        mock_sf_class.return_value = mock_connector
        
        cc = CloudConduit()
        sf1 = cc.snowflake("test-user")
        sf2 = cc.snowflake("test-user")
        
        assert sf1 is sf2
        mock_sf_class.assert_called_once()  # Should only create once

    @patch('cloudconduit.utils.unified_api.DatabricksConnector')
    def test_databricks_connector_creation(self, mock_db_class):
        """Test Databricks connector creation."""
        mock_connector = Mock()
        mock_db_class.return_value = mock_connector
        
        cc = CloudConduit()
        db = cc.databricks()
        
        assert db is mock_connector
        assert cc._databricks is mock_connector
        mock_db_class.assert_called_once_with(None)

    @patch('cloudconduit.utils.unified_api.S3Connector')
    def test_s3_connector_creation(self, mock_s3_class):
        """Test S3 connector creation."""
        mock_connector = Mock()
        mock_s3_class.return_value = mock_connector
        
        cc = CloudConduit()
        s3 = cc.s3()
        
        assert s3 is mock_connector
        assert cc._s3 is mock_connector
        mock_s3_class.assert_called_once_with(None)


class TestConvenienceFunctions:
    """Test convenience functions."""

    @patch('cloudconduit.utils.unified_api.SnowflakeConnector')
    def test_connect_snowflake_with_username(self, mock_sf_class):
        """Test connect_snowflake function with explicit username."""
        mock_connector = Mock()
        mock_sf_class.return_value = mock_connector
        
        sf = connect_snowflake("test-user", {"warehouse": "test"})
        
        assert sf is mock_connector
        mock_sf_class.assert_called_once_with("test-user", {"warehouse": "test"})
        mock_connector.connect.assert_called_once()

    @patch('cloudconduit.utils.unified_api.SnowflakeConnector')
    def test_connect_snowflake_default_username(self, mock_sf_class):
        """Test connect_snowflake function with default username."""
        mock_connector = Mock()
        mock_sf_class.return_value = mock_connector
        
        sf = connect_snowflake()
        
        assert sf is mock_connector
        mock_sf_class.assert_called_once_with(None, None)
        mock_connector.connect.assert_called_once()

    @patch('cloudconduit.utils.unified_api.DatabricksConnector')
    def test_connect_databricks(self, mock_db_class):
        """Test connect_databricks function."""
        mock_connector = Mock()
        mock_db_class.return_value = mock_connector
        
        db = connect_databricks({"catalog": "test"})
        
        assert db is mock_connector
        mock_db_class.assert_called_once_with({"catalog": "test"})
        mock_connector.connect.assert_called_once()

    @patch('cloudconduit.utils.unified_api.S3Connector')
    def test_connect_s3(self, mock_s3_class):
        """Test connect_s3 function."""
        mock_connector = Mock()
        mock_s3_class.return_value = mock_connector
        
        s3 = connect_s3({"region_name": "us-west-2"})
        
        assert s3 is mock_connector
        mock_s3_class.assert_called_once_with({"region_name": "us-west-2"})
        mock_connector.connect.assert_called_once()

    def test_execute_snowflake(self):
        """Test execute with Snowflake connector."""
        mock_connector = Mock(spec=SnowflakeConnector)
        mock_result = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        mock_connector.execute.return_value = mock_result
        
        result = execute(mock_connector, "SELECT * FROM test")
        
        assert result is mock_result
        mock_connector.execute.assert_called_once_with("SELECT * FROM test")

    def test_execute_databricks(self):
        """Test execute with Databricks connector."""
        mock_connector = Mock(spec=DatabricksConnector)
        mock_result = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        mock_connector.execute.return_value = mock_result
        
        result = execute(mock_connector, "SELECT * FROM test", limit=10)
        
        assert result is mock_result
        mock_connector.execute.assert_called_once_with("SELECT * FROM test", limit=10)

    def test_upload_df_database(self, sample_dataframe):
        """Test upload_df with database connector."""
        mock_connector = Mock(spec=SnowflakeConnector)
        
        upload_df(mock_connector, sample_dataframe, "test_table")
        
        mock_connector.upload_df.assert_called_once_with(sample_dataframe, "test_table")

    def test_upload_df_s3(self, sample_dataframe):
        """Test upload_df with S3 connector."""
        mock_connector = Mock(spec=S3Connector)
        
        upload_df(mock_connector, sample_dataframe, "bucket/key.csv")
        
        mock_connector.upload_df.assert_called_once_with(sample_dataframe, "bucket", "key.csv")

    def test_upload_df_s3_invalid_destination(self, sample_dataframe):
        """Test upload_df with S3 connector and invalid destination."""
        mock_connector = Mock(spec=S3Connector)
        
        with pytest.raises(ValueError, match="S3 destination must be in format 'bucket/key'"):
            upload_df(mock_connector, sample_dataframe, "invalid-destination")


    def test_copy_table(self):
        """Test copy_table function."""
        mock_connector = Mock()
        
        copy_table(mock_connector, "source", "target", if_exists="replace")
        
        mock_connector.copy_table.assert_called_once_with("source", "target", if_exists="replace")

    def test_drop_table(self):
        """Test drop_table function."""
        mock_connector = Mock()
        
        drop_table(mock_connector, "test_table")
        
        mock_connector.drop_table.assert_called_once_with("test_table")

    def test_grant_access(self):
        """Test grant_access function."""
        mock_connector = Mock()
        
        grant_access(mock_connector, "test_table", "user", "SELECT,INSERT")
        
        mock_connector.grant_access.assert_called_once_with(
            "test_table", "user", "SELECT,INSERT"
        )

    def test_grant_access_with_defaults(self):
        """Test grant_access function with default privileges."""
        mock_connector = Mock()
        
        grant_access(mock_connector, "test_table", "user")
        
        mock_connector.grant_access.assert_called_once_with(
            "test_table", "user", "SELECT"
        )