"""Unit tests for S3Connector."""

import pytest
import pandas as pd
from unittest.mock import Mock, patch
import io

from cloudconduit.connectors.s3 import S3Connector


class TestS3Connector:
    """Test S3Connector functionality."""

    def test_init(self):
        """Test connector initialization."""
        connector = S3Connector()
        assert connector._connection is None
        assert connector._s3_client is None
        assert connector._s3_resource is None

    def test_init_with_config(self):
        """Test connector initialization with config."""
        config = {"region_name": "us-west-2"}
        connector = S3Connector(config)
        assert "region_name" in connector.config

    @patch('boto3.Session')
    def test_connect_success(self, mock_session, mock_s3_client, mock_environment):
        """Test successful connection."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_instance.resource.return_value = Mock()
        
        connector = S3Connector()
        connector.connect()
        
        assert connector._connection is not None
        assert connector._s3_client is not None
        mock_s3_client.list_buckets.assert_called_once()  # Test connection

    @patch('boto3.Session')
    def test_connect_failure(self, mock_session, mock_environment):
        """Test connection failure."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_s3_client = Mock()
        mock_session_instance.client.return_value = mock_s3_client
        mock_s3_client.list_buckets.side_effect = Exception("Connection failed")
        
        connector = S3Connector()
        
        with pytest.raises(ConnectionError, match="Failed to connect to S3"):
            connector.connect()

    def test_disconnect(self):
        """Test disconnection."""
        connector = S3Connector()
        connector._connection = Mock()
        connector._s3_client = Mock()
        connector._s3_resource = Mock()
        
        connector.disconnect()
        
        assert connector._connection is None
        assert connector._s3_client is None
        assert connector._s3_resource is None

    @patch('boto3.Session')
    def test_execute_operation(self, mock_session, mock_s3_client, mock_environment):
        """Test executing S3 operations."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_instance.resource.return_value = Mock()
        
        connector = S3Connector()
        result = connector.execute("list_buckets")
        
        assert result == mock_s3_client.list_buckets.return_value
        mock_s3_client.list_buckets.assert_called()

    @patch('boto3.Session')
    def test_execute_operation_with_params(self, mock_session, mock_s3_client, mock_environment):
        """Test executing S3 operations with parameters."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_instance.resource.return_value = Mock()
        
        connector = S3Connector()
        connector.execute("list_objects_v2", Bucket="test-bucket", Prefix="data/")
        
        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket="test-bucket", Prefix="data/"
        )

    @patch('boto3.Session')
    def test_upload_df_csv(self, mock_session, mock_s3_client, sample_dataframe, mock_environment):
        """Test uploading DataFrame as CSV."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_instance.resource.return_value = Mock()
        
        connector = S3Connector()
        connector.upload_df(sample_dataframe, "test-bucket", "data/file.csv", file_format="csv")
        
        mock_s3_client.put_object.assert_called_once()
        call_args = mock_s3_client.put_object.call_args
        assert call_args[1]["Bucket"] == "test-bucket"
        assert call_args[1]["Key"] == "data/file.csv"

    @patch('boto3.Session')
    def test_upload_df_parquet(self, mock_session, mock_s3_client, sample_dataframe, mock_environment):
        """Test uploading DataFrame as Parquet."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_instance.resource.return_value = Mock()
        
        connector = S3Connector()
        connector.upload_df(sample_dataframe, "test-bucket", "data/file.parquet", file_format="parquet")
        
        mock_s3_client.put_object.assert_called_once()

    @patch('boto3.Session')
    def test_upload_df_unsupported_format(self, mock_session, mock_s3_client, 
                                         sample_dataframe, mock_environment):
        """Test uploading DataFrame with unsupported format."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_instance.resource.return_value = Mock()
        
        connector = S3Connector()
        
        with pytest.raises(ValueError, match="Unsupported file format"):
            connector.upload_df(sample_dataframe, "test-bucket", "data/file.xml", file_format="xml")

    @patch('boto3.Session')
    def test_download_df_csv(self, mock_session, mock_s3_client, mock_environment):
        """Test downloading DataFrame from CSV."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_instance.resource.return_value = Mock()
        
        connector = S3Connector()
        result = connector.download_df("test-bucket", "data/file.csv", file_format="csv")
        
        assert isinstance(result, pd.DataFrame)
        mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="data/file.csv"
        )

    @patch('boto3.Session')
    def test_list_buckets(self, mock_session, mock_s3_client, mock_environment):
        """Test listing buckets."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_instance.resource.return_value = Mock()
        
        connector = S3Connector()
        result = connector.list_buckets()
        
        expected = [
            {'Name': 'bucket1', 'CreationDate': '2023-01-01'},
            {'Name': 'bucket2', 'CreationDate': '2023-01-02'}
        ]
        assert result == expected

    @patch('boto3.Session')
    def test_list_objects(self, mock_session, mock_s3_client, mock_environment):
        """Test listing objects."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_instance.resource.return_value = Mock()
        
        connector = S3Connector()
        result = connector.list_objects("test-bucket", prefix="data/")
        
        expected = [
            {'Key': 'file1.csv', 'Size': 1024},
            {'Key': 'file2.parquet', 'Size': 2048}
        ]
        assert result == expected

    @patch('boto3.Session')
    def test_copy_object(self, mock_session, mock_s3_client, mock_environment):
        """Test copying object."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_instance.resource.return_value = Mock()
        
        connector = S3Connector()
        connector.copy_object("src-bucket", "src/file.csv", "dst-bucket", "dst/file.csv")
        
        mock_s3_client.copy_object.assert_called_once()

    @patch('boto3.Session')
    def test_delete_object(self, mock_session, mock_s3_client, mock_environment):
        """Test deleting object."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_instance.resource.return_value = Mock()
        
        connector = S3Connector()
        connector.delete_object("test-bucket", "file.csv")
        
        mock_s3_client.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key="file.csv"
        )

    @patch('boto3.Session')
    def test_copy_table_with_path(self, mock_session, mock_s3_client, mock_environment):
        """Test copy_table method with S3 paths."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_instance.resource.return_value = Mock()
        
        connector = S3Connector()
        connector.copy_table("src-bucket/src/file.csv", "dst-bucket/dst/file.csv")
        
        mock_s3_client.copy_object.assert_called_once()

    @patch('boto3.Session')
    def test_drop_table_with_path(self, mock_session, mock_s3_client, mock_environment):
        """Test drop_table method with S3 path."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_instance.resource.return_value = Mock()
        
        connector = S3Connector()
        connector.drop_table("test-bucket/file.csv")
        
        mock_s3_client.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key="file.csv"
        )

    @patch('boto3.Session')
    def test_create_presigned_url(self, mock_session, mock_s3_client, mock_environment):
        """Test creating presigned URL."""
        mock_session_instance = Mock()
        mock_session.return_value = mock_session_instance
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_instance.resource.return_value = Mock()
        mock_s3_client.generate_presigned_url.return_value = "https://presigned-url.com"
        
        connector = S3Connector()
        url = connector.create_presigned_url("test-bucket", "file.csv", expiration=1800)
        
        assert url == "https://presigned-url.com"
        mock_s3_client.generate_presigned_url.assert_called_once_with(
            "get_object",
            Params={"Bucket": "test-bucket", "Key": "file.csv"},
            ExpiresIn=1800
        )

    def test_parse_s3_path_simple(self):
        """Test parsing simple S3 path."""
        connector = S3Connector()
        bucket, key = connector._parse_s3_path("my-bucket/path/to/file.csv")
        assert bucket == "my-bucket"
        assert key == "path/to/file.csv"

    def test_parse_s3_path_with_protocol(self):
        """Test parsing S3 path with s3:// protocol."""
        connector = S3Connector()
        bucket, key = connector._parse_s3_path("s3://my-bucket/path/to/file.csv")
        assert bucket == "my-bucket"
        assert key == "path/to/file.csv"

    def test_parse_s3_path_invalid(self):
        """Test parsing invalid S3 path."""
        connector = S3Connector()
        
        with pytest.raises(ValueError, match="Invalid S3 path format"):
            connector._parse_s3_path("invalid-path")

    def test_grant_access_placeholder(self):
        """Test grant_access placeholder implementation."""
        connector = S3Connector()
        # Should not raise error, just print message
        connector.grant_access("test-bucket", "arn:aws:iam::123:user/test", "READ")