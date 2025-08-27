"""Pytest configuration and fixtures."""

import os
import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock, patch
from typing import Dict, Any

from cloudconduit.utils.config_manager import ConfigManager


@pytest.fixture
def sample_dataframe():
    """Sample DataFrame for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'value': [100.5, 200.0, 300.25, 400.75, 500.0],
        'active': [True, False, True, True, False],
        'created_at': pd.date_range('2023-01-01', periods=5, freq='D')
    })


@pytest.fixture
def mock_snowflake_connection():
    """Mock Snowflake connection."""
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    
    # Mock cursor methods
    mock_cursor.description = [('ID', 'NUMBER'), ('NAME', 'VARCHAR'), ('VALUE', 'FLOAT')]
    mock_cursor.fetchall.return_value = [(1, 'Alice', 100.5), (2, 'Bob', 200.0)]
    mock_cursor.execute.return_value = None
    mock_cursor.close.return_value = None
    
    mock_conn.close.return_value = None
    return mock_conn


@pytest.fixture
def mock_databricks_connection():
    """Mock Databricks connection."""
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    
    # Mock cursor methods
    mock_cursor.description = [('table_name', 'STRING'), ('database', 'STRING')]
    mock_cursor.fetchall.return_value = [('test_table', 'default'), ('users', 'default')]
    mock_cursor.execute.return_value = None
    mock_cursor.close.return_value = None
    
    mock_conn.close.return_value = None
    return mock_conn


@pytest.fixture
def mock_s3_client():
    """Mock S3 client."""
    mock_client = Mock()
    
    # Mock S3 operations
    mock_client.list_buckets.return_value = {
        'Buckets': [
            {'Name': 'bucket1', 'CreationDate': '2023-01-01'},
            {'Name': 'bucket2', 'CreationDate': '2023-01-02'}
        ]
    }
    
    mock_client.list_objects_v2.return_value = {
        'Contents': [
            {'Key': 'file1.csv', 'Size': 1024},
            {'Key': 'file2.parquet', 'Size': 2048}
        ]
    }
    
    mock_client.put_object.return_value = {'ETag': 'mock-etag'}
    mock_client.get_object.return_value = {
        'Body': Mock(read=lambda: b'id,name,value\n1,Alice,100\n2,Bob,200\n')
    }
    
    return mock_client


@pytest.fixture
def mock_credentials():
    """Mock credentials dictionary."""
    return {
        'snowflake': {
            'account': 'test-account',
            'user': 'test-user',
            'password': 'test-password',
            'warehouse': 'test-warehouse',
            'database': 'test-database',
            'schema': 'test-schema'
        },
        'databricks': {
            'server_hostname': 'test-workspace.cloud.databricks.com',
            'http_path': '/sql/1.0/warehouses/test-warehouse',
            'access_token': 'test-token',
            'catalog': 'test-catalog',
            'schema': 'test-schema'
        },
        's3': {
            'aws_access_key_id': 'test-access-key',
            'aws_secret_access_key': 'test-secret-key',
            'region_name': 'us-east-1'
        }
    }


@pytest.fixture
def mock_environment(mock_credentials):
    """Mock environment variables."""
    env_vars = {
        'SNOWFLAKE_ACCOUNT': mock_credentials['snowflake']['account'],
        'SNOWFLAKE_PASSWORD': mock_credentials['snowflake']['password'],
        'SNOWFLAKE_WAREHOUSE': mock_credentials['snowflake']['warehouse'],
        'SNOWFLAKE_DATABASE': mock_credentials['snowflake']['database'],
        'SNOWFLAKE_SCHEMA': mock_credentials['snowflake']['schema'],
        
        'DATABRICKS_SERVER_HOSTNAME': mock_credentials['databricks']['server_hostname'],
        'DATABRICKS_HTTP_PATH': mock_credentials['databricks']['http_path'],
        'DATABRICKS_ACCESS_TOKEN': mock_credentials['databricks']['access_token'],
        'DATABRICKS_CATALOG': mock_credentials['databricks']['catalog'],
        'DATABRICKS_SCHEMA': mock_credentials['databricks']['schema'],
        
        'AWS_ACCESS_KEY_ID': mock_credentials['s3']['aws_access_key_id'],
        'AWS_SECRET_ACCESS_KEY': mock_credentials['s3']['aws_secret_access_key'],
        'AWS_DEFAULT_REGION': mock_credentials['s3']['region_name'],
    }
    
    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def mock_keyring():
    """Mock keyring functionality."""
    with patch('keyring.get_password') as mock_get, \
         patch('keyring.set_password') as mock_set, \
         patch('keyring.delete_password') as mock_delete:
        
        mock_get.return_value = 'mock-password'
        mock_set.return_value = None
        mock_delete.return_value = None
        
        yield {
            'get_password': mock_get,
            'set_password': mock_set,
            'delete_password': mock_delete
        }


@pytest.fixture
def config_manager(mock_environment, mock_keyring):
    """ConfigManager instance with mocked dependencies."""
    return ConfigManager()


@pytest.fixture(scope="session")
def test_data_dir(tmp_path_factory):
    """Create temporary directory for test data."""
    return tmp_path_factory.mktemp("test_data")