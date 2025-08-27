"""Integration tests for CloudConduit connectors."""

import pytest
import pandas as pd
import os
from unittest.mock import patch

from cloudconduit import (
    connect_snowflake,
    connect_databricks, 
    connect_s3,
    CloudConduit
)


@pytest.mark.integration
@pytest.mark.requires_credentials
class TestSnowflakeIntegration:
    """Integration tests for Snowflake connector."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment."""
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.username = os.getenv("SNOWFLAKE_USER")
        
        if not self.account or not self.username:
            pytest.skip("Snowflake credentials not configured")

    def test_connection_and_basic_query(self):
        """Test Snowflake connection and basic query."""
        with connect_snowflake(self.account, self.username) as sf:
            result = sf.execute("SELECT CURRENT_USER(), CURRENT_ROLE()")
            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0

    def test_upload_and_query_dataframe(self):
        """Test uploading DataFrame and querying it back."""
        test_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100.5, 200.0, 300.25]
        })
        
        with connect_snowflake(self.account, self.username) as sf:
            table_name = "test_cloudconduit_temp"
            
            try:
                # Upload DataFrame
                sf.upload_df(test_df, table_name, if_exists="replace")
                
                # Query back
                result = sf.execute(f"SELECT * FROM {table_name} ORDER BY id")
                
                # Verify data
                assert len(result) == 3
                assert result.iloc[0]['ID'] == 1
                assert result.iloc[0]['NAME'] == 'Alice'
                
            finally:
                # Cleanup
                sf.drop_table(table_name)

    def test_table_operations(self):
        """Test table copy and grant operations."""
        test_df = pd.DataFrame({'id': [1], 'value': [100]})
        
        with connect_snowflake(self.account, self.username) as sf:
            source_table = "test_source_temp"
            target_table = "test_target_temp"
            
            try:
                # Create source table
                sf.upload_df(test_df, source_table)
                
                # Copy table
                sf.copy_table(source_table, target_table)
                
                # Verify copy
                result = sf.execute(f"SELECT COUNT(*) as cnt FROM {target_table}")
                assert result.iloc[0]['CNT'] == 1
                
                # Test grant (may fail depending on permissions)
                try:
                    sf.grant_access(target_table, "PUBLIC", "SELECT")
                except Exception:
                    pass  # Grant may fail due to permissions
                
            finally:
                # Cleanup
                sf.drop_table(source_table)
                sf.drop_table(target_table)


@pytest.mark.integration  
@pytest.mark.requires_credentials
class TestDatabricksIntegration:
    """Integration tests for Databricks connector."""
    
    @pytest.fixture(autouse=True) 
    def setup(self):
        """Setup test environment."""
        self.server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        self.http_path = os.getenv("DATABRICKS_HTTP_PATH")
        self.access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
        
        if not all([self.server_hostname, self.http_path, self.access_token]):
            pytest.skip("Databricks credentials not configured")

    def test_connection_and_basic_query(self):
        """Test Databricks connection and basic query."""
        with connect_databricks() as db:
            result = db.execute("SELECT current_user() as user")
            assert isinstance(result, pd.DataFrame)
            assert len(result) > 0

    def test_list_tables_and_schemas(self):
        """Test listing tables and schemas."""
        with connect_databricks() as db:
            # List schemas
            schemas = db.list_schemas()
            assert isinstance(schemas, pd.DataFrame)
            
            # List tables (may be empty)
            tables = db.list_tables()
            assert isinstance(tables, pd.DataFrame)


@pytest.mark.integration
@pytest.mark.requires_credentials  
class TestS3Integration:
    """Integration tests for S3 connector."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment."""
        self.access_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        if not self.access_key or not self.secret_key:
            pytest.skip("AWS credentials not configured")

    def test_connection_and_list_buckets(self):
        """Test S3 connection and listing buckets."""
        with connect_s3() as s3:
            buckets = s3.list_buckets()
            assert isinstance(buckets, list)

    def test_upload_download_dataframe(self):
        """Test uploading and downloading DataFrame."""
        test_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100.5, 200.0, 300.25]
        })
        
        # This test requires a test bucket - skip if not available
        test_bucket = os.getenv("AWS_TEST_BUCKET")
        if not test_bucket:
            pytest.skip("AWS test bucket not configured")
        
        with connect_s3() as s3:
            key = "test/cloudconduit_test.csv"
            
            try:
                # Upload DataFrame
                s3.upload_df(test_df, test_bucket, key, file_format="csv")
                
                # Download and verify
                result = s3.download_df(test_bucket, key, file_format="csv")
                
                assert len(result) == 3
                assert list(result.columns) == ['id', 'name', 'value']
                
            finally:
                # Cleanup
                try:
                    s3.delete_object(test_bucket, key)
                except Exception:
                    pass

    def test_object_operations(self):
        """Test S3 object operations."""
        test_bucket = os.getenv("AWS_TEST_BUCKET")
        if not test_bucket:
            pytest.skip("AWS test bucket not configured")
        
        with connect_s3() as s3:
            # List objects
            objects = s3.list_objects(test_bucket)
            assert isinstance(objects, list)
            
            # Test presigned URL generation
            if objects:
                key = objects[0]['Key']
                url = s3.create_presigned_url(test_bucket, key, expiration=300)
                assert url.startswith("https://")


@pytest.mark.integration
class TestUnifiedInterface:
    """Integration tests for unified interface."""

    def test_cloud_conduit_interface(self):
        """Test CloudConduit unified interface."""
        cc = CloudConduit()
        
        # Test connector creation (without actual connection)
        if os.getenv("SNOWFLAKE_ACCOUNT") and os.getenv("SNOWFLAKE_USER"):
            sf = cc.snowflake(os.getenv("SNOWFLAKE_ACCOUNT"), os.getenv("SNOWFLAKE_USER"))
            assert sf is not None
            
        if all([os.getenv("DATABRICKS_SERVER_HOSTNAME"), 
                os.getenv("DATABRICKS_HTTP_PATH"),
                os.getenv("DATABRICKS_ACCESS_TOKEN")]):
            db = cc.databricks()
            assert db is not None
            
        if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
            s3 = cc.s3()
            assert s3 is not None


@pytest.mark.slow
class TestPerformance:
    """Performance tests for CloudConduit."""
    
    def test_large_dataframe_operations(self):
        """Test operations with large DataFrames."""
        import time
        
        # Create large DataFrame
        large_df = pd.DataFrame({
            'id': range(10000),
            'data': ['test_data'] * 10000,
            'value': [i * 0.1 for i in range(10000)]
        })
        
        start_time = time.time()
        
        # Test CSV serialization (without actual upload)
        csv_buffer = large_df.to_csv(index=False)
        
        elapsed = time.time() - start_time
        
        # Should complete in reasonable time (< 5 seconds)
        assert elapsed < 5.0
        assert len(csv_buffer) > 0