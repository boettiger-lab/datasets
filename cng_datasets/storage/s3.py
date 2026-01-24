"""
S3 bucket management utilities.

Tools for creating, configuring, and managing S3 buckets for
public dataset distribution.
"""

from typing import Optional, Dict, Any, List
import json
import os
import duckdb


def configure_s3_credentials(con) -> None:
    """
    Configure S3 credentials for DuckDB using environment variables.
    
    Args:
        con: DuckDB connection (raw or ibis) to configure
    """
    # Install and load httpfs extension for S3/HTTP access FIRST
    # Handle both raw DuckDB and Ibis connections
    if hasattr(con, 'raw_sql'):
        # Ibis connection
        con.raw_sql("INSTALL httpfs")
        con.raw_sql("LOAD httpfs")
    else:
        # Raw DuckDB connection
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
    
    # Enable large buffer size for complex geometries
    if hasattr(con, 'raw_sql'):
        con.raw_sql("SET arrow_large_buffer_size=true")
    else:
        con.execute("SET arrow_large_buffer_size=true")
    
    key = os.getenv("AWS_ACCESS_KEY_ID", "")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    endpoint = os.getenv("AWS_S3_ENDPOINT", "s3-west.nrp-nautilus.io")
    region = os.getenv("AWS_REGION", "us-east-1")
    use_ssl = os.getenv("AWS_HTTPS", "TRUE")
    url_style = os.getenv("AWS_VIRTUAL_HOSTING", "FALSE")
    
    # Convert TRUE/FALSE strings to path/vhost
    if url_style.upper() == "FALSE":
        url_style = "path"
    else:
        url_style = "vhost"
    
    # Always create secret, even for anonymous/public access (empty key/secret)
    # USE_SSL must be a string, not boolean!
    query = f"""
    CREATE OR REPLACE SECRET s3_secret (
        TYPE S3,
        KEY_ID '{key}',
        SECRET '{secret}',
        USE_SSL '{use_ssl}',
        ENDPOINT '{endpoint}',
        URL_STYLE '{url_style}',
        REGION '{region}'
    );
    """
    if hasattr(con, 'raw_sql'):
        con.raw_sql(query)
    else:
        con.execute(query)


class S3Manager:
    """
    Manage S3 buckets for dataset storage.
    
    Handles bucket creation, CORS configuration, and public access setup.
    """
    
    def __init__(
        self,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        region: str = "us-west-2",
    ):
        """
        Initialize S3 manager.
        
        Args:
            access_key: AWS access key ID
            secret_key: AWS secret access key
            endpoint_url: S3-compatible endpoint URL (for non-AWS providers)
            region: AWS region
        """
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self.region = region
        self._client = None
    
    @property
    def client(self):
        """Get or create boto3 S3 client."""
        if self._client is None:
            import boto3
            self._client = boto3.client(
                's3',
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                endpoint_url=self.endpoint_url,
                region_name=self.region,
            )
        return self._client
    
    def create_bucket(self, bucket_name: str, public: bool = False) -> str:
        """
        Create an S3 bucket.
        
        Args:
            bucket_name: Name for the bucket
            public: Whether to configure for public read access
            
        Returns:
            Bucket name
        """
        # Placeholder - to be implemented with boto3
        raise NotImplementedError("Bucket creation to be implemented")
    
    def configure_cors(
        self,
        bucket_name: str,
        allowed_origins: Optional[List[str]] = None,
        allowed_methods: Optional[List[str]] = None,
    ):
        """
        Configure CORS for a bucket.
        
        Args:
            bucket_name: Bucket name
            allowed_origins: List of allowed origins (default: ["*"])
            allowed_methods: List of allowed methods (default: ["GET", "HEAD"])
        """
        if allowed_origins is None:
            allowed_origins = ["*"]
        if allowed_methods is None:
            allowed_methods = ["GET", "HEAD"]
        
        cors_configuration = {
            'CORSRules': [{
                'AllowedOrigins': allowed_origins,
                'AllowedMethods': allowed_methods,
                'AllowedHeaders': ['*'],
                'MaxAgeSeconds': 3600
            }]
        }
        
        self.client.put_bucket_cors(
            Bucket=bucket_name,
            CORSConfiguration=cors_configuration
        )
        print(f"CORS configured for bucket: {bucket_name}")
    
    def set_public_read(self, bucket_name: str):
        """
        Configure bucket for public read access.
        
        Args:
            bucket_name: Bucket name
        """
        policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{bucket_name}/*"
            }]
        }
        
        self.client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps(policy)
        )
        print(f"Public read access enabled for bucket: {bucket_name}")


def configure_bucket_cors(
    bucket_name: str,
    endpoint_url: Optional[str] = None,
    **kwargs
):
    """
    Convenience function to configure CORS for a bucket.
    
    Args:
        bucket_name: Bucket name
        endpoint_url: S3-compatible endpoint URL
        **kwargs: Additional arguments passed to S3Manager
    """
    manager = S3Manager(endpoint_url=endpoint_url, **kwargs)
    manager.configure_cors(bucket_name)


def create_public_bucket(
    bucket_name: str,
    endpoint_url: Optional[str] = None,
    **kwargs
) -> str:
    """
    Create a publicly accessible S3 bucket.
    
    Args:
        bucket_name: Name for the bucket
        endpoint_url: S3-compatible endpoint URL
        **kwargs: Additional arguments passed to S3Manager
        
    Returns:
        Bucket name
    """
    manager = S3Manager(endpoint_url=endpoint_url, **kwargs)
    return manager.create_bucket(bucket_name, public=True)
