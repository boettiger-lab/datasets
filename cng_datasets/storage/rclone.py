"""
Rclone synchronization utilities.

Tools for syncing datasets across multiple S3-compatible storage providers
using rclone.
"""

from typing import Optional, List, Dict
import subprocess
import os
from pathlib import Path


def create_public_bucket(
    bucket_name: str,
    remote: str = "nrp",
    set_cors: bool = True,
) -> bool:
    """
    Create a public bucket with rclone and set it to public read access.
    
    Args:
        bucket_name: Name of the bucket to create
        remote: Rclone remote name (default: nrp)
        set_cors: Whether to set CORS headers (default: True)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create bucket
        print(f"Creating bucket: {bucket_name}")
        result = subprocess.run(
            ["rclone", "mkdir", f"{remote}:{bucket_name}"],
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.returncode != 0 and "already exists" not in result.stderr.lower():
            print(f"Error creating bucket: {result.stderr}")
            return False
        
        # Set public read policy using AWS CLI
        endpoint = os.getenv("AWS_PUBLIC_ENDPOINT", "s3-west.nrp-nautilus.io")
        
        print(f"Setting public read access for bucket: {bucket_name}")
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": [
                        "s3:GetBucketLocation",
                        "s3:ListBucket"
                    ],
                    "Resource": [f"arn:aws:s3:::{bucket_name}"]
                },
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*"]
                }
            ]
        }
        
        import json
        policy_json = json.dumps(policy)
        
        # Build environment with AWS credentials explicitly
        env = os.environ.copy()
        
        result = subprocess.run(
            [
                "aws", "s3api", "put-bucket-policy",
                "--bucket", bucket_name,
                "--policy", policy_json,
                "--endpoint-url", f"https://{endpoint}"
            ],
            capture_output=True,
            text=True,
            check=False,
            env=env
        )
        
        if result.returncode != 0:
            print(f"Warning: Could not set public policy: {result.stderr}")
        else:
            print(f"✓ Public read access enabled for bucket: {bucket_name}")
        
        # Set CORS if requested
        if set_cors:
            print(f"Setting CORS configuration for bucket: {bucket_name}")
            cors_config = {
                "CORSRules": [{
                    "AllowedOrigins": ["*"],
                    "AllowedMethods": ["GET", "HEAD"],
                    "AllowedHeaders": ["*"],
                    "ExposeHeaders": ["ETag", "Content-Type", "Content-Disposition"],
                    "MaxAgeSeconds": 3600
                }]
            }
            
            cors_json = json.dumps(cors_config)
            
            result = subprocess.run(
                [
                    "aws", "s3api", "put-bucket-cors",
                    "--bucket", bucket_name,
                    "--cors-configuration", cors_json,
                    "--endpoint-url", f"https://{endpoint}"
                ],
                capture_output=True,
                text=True,
                check=False,
                env=os.environ.copy()
            )
            
            if result.returncode != 0:
                print(f"Warning: Could not set CORS: {result.stderr}")
            else:
                print(f"✓ CORS configuration set for bucket: {bucket_name}")
        
        return True
        
    except Exception as e:
        print(f"Error creating public bucket: {e}")
        return False


class RcloneSync:
    """
    Manage rclone synchronization of datasets.
    
    Handles syncing between different S3-compatible storage providers.
    """
    
    def __init__(
        self,
        config_path: Optional[str] = None,
        dry_run: bool = False,
    ):
        """
        Initialize rclone sync manager.
        
        Args:
            config_path: Path to rclone config file
            dry_run: If True, only simulate sync operations
        """
        self.config_path = config_path or str(Path.home() / ".config" / "rclone" / "rclone.conf")
        self.dry_run = dry_run
    
    def sync(
        self,
        source: str,
        destination: str,
        filters: Optional[List[str]] = None,
        flags: Optional[List[str]] = None,
    ) -> subprocess.CompletedProcess:
        """
        Sync files from source to destination.
        
        Args:
            source: Source path (remote:bucket/path or local path)
            destination: Destination path (remote:bucket/path or local path)
            filters: List of filter patterns (--include, --exclude)
            flags: Additional rclone flags
            
        Returns:
            Completed process result
        """
        cmd = [
            "rclone", "sync",
            "--config", self.config_path,
            "--progress",
            "--checksum",
        ]
        
        if self.dry_run:
            cmd.append("--dry-run")
        
        if filters:
            cmd.extend(filters)
        
        if flags:
            cmd.extend(flags)
        
        cmd.extend([source, destination])
        
        print(f"Running: {' '.join(cmd)}")
        return subprocess.run(cmd, capture_output=True, text=True)
    
    def copy(
        self,
        source: str,
        destination: str,
        **kwargs
    ) -> subprocess.CompletedProcess:
        """
        Copy files from source to destination (doesn't delete from destination).
        
        Args:
            source: Source path
            destination: Destination path
            **kwargs: Additional arguments passed to sync
            
        Returns:
            Completed process result
        """
        # Similar to sync but uses 'copy' command
        raise NotImplementedError("Copy operation to be implemented")
    
    def configure_remote(
        self,
        remote_name: str,
        provider: str,
        access_key: str,
        secret_key: str,
        endpoint: Optional[str] = None,
        region: Optional[str] = None,
    ):
        """
        Configure a new rclone remote.
        
        Args:
            remote_name: Name for the remote
            provider: Provider type (s3, aws, cloudflare, etc.)
            access_key: Access key ID
            secret_key: Secret access key
            endpoint: Endpoint URL for S3-compatible services
            region: AWS region
        """
        # Placeholder - to be implemented
        raise NotImplementedError("Remote configuration to be implemented")


def sync_to_providers(
    source_bucket: str,
    source_remote: str,
    target_remotes: List[str],
    path: str = "",
    **kwargs
):
    """
    Sync a dataset from source to multiple target providers.
    
    Args:
        source_bucket: Source bucket name
        source_remote: Source rclone remote name
        target_remotes: List of target rclone remote names
        path: Path within buckets to sync
        **kwargs: Additional arguments passed to RcloneSync
    """
    syncer = RcloneSync(**kwargs)
    
    source_path = f"{source_remote}:{source_bucket}"
    if path:
        source_path += f"/{path}"
    
    results = []
    for target_remote in target_remotes:
        target_path = f"{target_remote}:{source_bucket}"
        if path:
            target_path += f"/{path}"
        
        print(f"\nSyncing to {target_remote}...")
        result = syncer.sync(source_path, target_path)
        results.append((target_remote, result))
    
    return results
