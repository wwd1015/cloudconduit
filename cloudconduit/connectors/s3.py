"""S3 connector with credential management."""

from typing import Any, Dict, List, Optional, Union
import pandas as pd
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import io

from .base import BaseConnector
from ..utils.config_manager import ConfigManager


class S3Connector(BaseConnector):
    """S3 connector with credential management."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize S3 connector.
        
        Args:
            config: Additional configuration parameters (highest priority)
        """
        super().__init__(config)
        
        # Initialize configuration manager
        self.config_manager = ConfigManager()
        
        # Get complete configuration following priority order
        self.config = self.config_manager.get_s3_config(config)
        
        self._s3_client = None
        self._s3_resource = None

    def connect(self) -> None:
        """Establish connection to S3."""
        if self._connection:
            return
            
        try:
            # Create session with credentials
            session_params = {}
            
            if self.config.get("aws_access_key_id"):
                session_params["aws_access_key_id"] = self.config["aws_access_key_id"]
            if self.config.get("aws_secret_access_key"):
                session_params["aws_secret_access_key"] = self.config["aws_secret_access_key"]
            if self.config.get("aws_session_token"):
                session_params["aws_session_token"] = self.config["aws_session_token"]
            if self.config.get("region_name"):
                session_params["region_name"] = self.config["region_name"]
            
            session = boto3.Session(**session_params)
            self._s3_client = session.client('s3')
            self._s3_resource = session.resource('s3')
            self._connection = self._s3_client
            
            # Test connection
            self._s3_client.list_buckets()
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to S3: {e}")

    def disconnect(self) -> None:
        """Close connection to S3."""
        self._connection = None
        self._s3_client = None
        self._s3_resource = None

    def execute(self, operation: str, **kwargs) -> Any:
        """Execute S3 operation.
        
        Args:
            operation: S3 operation name (e.g., 'list_buckets', 'list_objects_v2')
            **kwargs: Parameters for the operation
            
        Returns:
            Operation result
        """
        if not self._connection:
            self.connect()
            
        try:
            method = getattr(self._s3_client, operation)
            return method(**kwargs)
        except Exception as e:
            raise RuntimeError(f"S3 operation '{operation}' failed: {e}")

    def upload_df(self, df: pd.DataFrame, bucket: str, key: str, file_format: str = "csv", **kwargs) -> None:
        """Upload pandas DataFrame to S3.
        
        Args:
            df: DataFrame to upload
            bucket: S3 bucket name
            key: S3 object key (file path)
            file_format: File format ('csv', 'parquet', 'json')
            **kwargs: Additional parameters
        """
        if not self._connection:
            self.connect()
            
        try:
            buffer = io.BytesIO()
            
            if file_format.lower() == "csv":
                df.to_csv(buffer, index=False, **kwargs)
            elif file_format.lower() == "parquet":
                df.to_parquet(buffer, index=False, **kwargs)
            elif file_format.lower() == "json":
                df.to_json(buffer, orient="records", **kwargs)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            buffer.seek(0)
            self._s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=buffer.getvalue()
            )
            
        except ValueError:
            # Re-raise ValueError for unsupported formats
            raise
        except Exception as e:
            raise RuntimeError(f"DataFrame upload to S3 failed: {e}")

    def download_df(self, bucket: str, key: str, file_format: str = "csv", **kwargs) -> pd.DataFrame:
        """Download S3 object as pandas DataFrame.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key (file path)
            file_format: File format ('csv', 'parquet', 'json')
            **kwargs: Additional parameters for pandas read functions
            
        Returns:
            Downloaded DataFrame
        """
        if not self._connection:
            self.connect()
            
        try:
            response = self._s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read()
            
            if file_format.lower() == "csv":
                return pd.read_csv(io.BytesIO(content), **kwargs)
            elif file_format.lower() == "parquet":
                return pd.read_parquet(io.BytesIO(content), **kwargs)
            elif file_format.lower() == "json":
                return pd.read_json(io.BytesIO(content), **kwargs)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
                
        except Exception as e:
            raise RuntimeError(f"DataFrame download from S3 failed: {e}")

    def list_buckets(self) -> List[Dict[str, Any]]:
        """List all S3 buckets.
        
        Returns:
            List of bucket information
        """
        result = self.execute("list_buckets")
        return result.get("Buckets", [])

    def list_objects(self, bucket: str, prefix: str = "", **kwargs) -> List[Dict[str, Any]]:
        """List objects in S3 bucket.
        
        Args:
            bucket: S3 bucket name
            prefix: Object key prefix filter
            **kwargs: Additional parameters
            
        Returns:
            List of object information
        """
        params = {"Bucket": bucket}
        if prefix:
            params["Prefix"] = prefix
        params.update(kwargs)
        
        result = self.execute("list_objects_v2", **params)
        return result.get("Contents", [])

    def copy_object(self, source_bucket: str, source_key: str, 
                   target_bucket: str, target_key: str, **kwargs) -> None:
        """Copy S3 object.
        
        Args:
            source_bucket: Source bucket name
            source_key: Source object key
            target_bucket: Target bucket name
            target_key: Target object key
            **kwargs: Additional parameters
        """
        copy_source = {"Bucket": source_bucket, "Key": source_key}
        self.execute("copy_object", 
                    CopySource=copy_source, 
                    Bucket=target_bucket, 
                    Key=target_key, 
                    **kwargs)

    def delete_object(self, bucket: str, key: str, **kwargs) -> None:
        """Delete S3 object.
        
        Args:
            bucket: S3 bucket name
            key: Object key to delete
            **kwargs: Additional parameters
        """
        self.execute("delete_object", Bucket=bucket, Key=key, **kwargs)

    def copy_table(self, source_path: str, target_path: str, **kwargs) -> None:
        """Copy S3 object (alias for copy_object with path format).
        
        Args:
            source_path: Source path in format 'bucket/key'
            target_path: Target path in format 'bucket/key'
            **kwargs: Additional parameters
        """
        source_bucket, source_key = self._parse_s3_path(source_path)
        target_bucket, target_key = self._parse_s3_path(target_path)
        
        self.copy_object(source_bucket, source_key, target_bucket, target_key, **kwargs)

    def drop_table(self, table_path: str, **kwargs) -> None:
        """Delete S3 object (alias for delete_object with path format).
        
        Args:
            table_path: Path in format 'bucket/key'
            **kwargs: Additional parameters
        """
        bucket, key = self._parse_s3_path(table_path)
        self.delete_object(bucket, key, **kwargs)

    def grant_access(self, bucket: str, user_or_role: str, privileges: str = "READ", **kwargs) -> None:
        """Grant access to S3 bucket (simplified IAM policy management).
        
        Note: This is a placeholder implementation. In practice, you would
        need to use IAM policies or bucket policies for access management.
        
        Args:
            bucket: Bucket name
            user_or_role: User or role ARN
            privileges: Access level ('READ', 'WRITE', 'FULL')
            **kwargs: Additional parameters
        """
        # This is a simplified example - real implementation would require
        # IAM policy management which is beyond the scope of this connector
        print(f"Note: Grant {privileges} access to {user_or_role} for bucket {bucket}")
        print("This requires IAM policy management outside of this connector.")

    def create_presigned_url(self, bucket: str, key: str, expiration: int = 3600, 
                           http_method: str = "GET") -> str:
        """Generate presigned URL for S3 object.
        
        Args:
            bucket: S3 bucket name
            key: Object key
            expiration: URL expiration time in seconds (default: 1 hour)
            http_method: HTTP method ('GET', 'PUT', etc.)
            
        Returns:
            Presigned URL
        """
        if not self._connection:
            self.connect()
            
        try:
            method_map = {
                "GET": "get_object",
                "PUT": "put_object",
                "DELETE": "delete_object"
            }
            
            client_method = method_map.get(http_method.upper(), "get_object")
            
            return self._s3_client.generate_presigned_url(
                client_method,
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=expiration
            )
            
        except Exception as e:
            raise RuntimeError(f"Failed to generate presigned URL: {e}")

    def _parse_s3_path(self, s3_path: str) -> tuple:
        """Parse S3 path into bucket and key.
        
        Args:
            s3_path: S3 path in format 'bucket/key' or 's3://bucket/key'
            
        Returns:
            Tuple of (bucket, key)
        """
        if s3_path.startswith("s3://"):
            s3_path = s3_path[5:]
        
        parts = s3_path.split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid S3 path format: {s3_path}")
        
        return parts[0], parts[1]