#!/usr/bin/env python3
"""
Setup S3 buckets with public access and CORS configuration.

This module handles the setup of S3 buckets before data conversion,
ensuring they have proper public read access and CORS headers configured.
"""

import subprocess
import json
import os
from typing import Optional


def setup_public_bucket(
    bucket_name: str,
    remote: str = "nrp",
    endpoint: Optional[str] = None,
    set_cors: bool = True,
    verbose: bool = True
) -> bool:
    """
    Create and configure a public S3 bucket with proper permissions.
    
    This function:
    1. Creates the bucket if it doesn't exist
    2. Sets public read policy (list bucket + get objects)
    3. Configures CORS headers for browser access
    
    Args:
        bucket_name: Name of the bucket to create/configure
        remote: Rclone remote name (default: nrp)
        endpoint: S3 endpoint URL (defaults to AWS_PUBLIC_ENDPOINT env var)
        set_cors: Whether to configure CORS headers (default: True)
        verbose: Print detailed progress (default: True)
        
    Returns:
        True if successful, False otherwise
        
    Example:
        >>> setup_public_bucket("my-data-bucket")
        Creating bucket: my-data-bucket
        ✓ Bucket created/verified
        ✓ Public read access enabled
        ✓ CORS configuration set
        True
    """
    # Get endpoint from environment if not specified
    if endpoint is None:
        endpoint = os.getenv("AWS_PUBLIC_ENDPOINT", "s3-west.nrp-nautilus.io")
    
    # Ensure AWS credentials are set
    if not os.getenv("AWS_ACCESS_KEY_ID") or not os.getenv("AWS_SECRET_ACCESS_KEY"):
        print("ERROR: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")
        return False
    
    try:
        # Step 1: Create bucket using rclone
        if verbose:
            print(f"Creating bucket: {bucket_name}")
        
        result = subprocess.run(
            ["rclone", "mkdir", f"{remote}:{bucket_name}"],
            capture_output=True,
            text=True,
            check=False,
            env=os.environ.copy()
        )
        
        if result.returncode != 0 and "already exists" not in result.stderr.lower():
            print(f"ERROR: Could not create bucket: {result.stderr}")
            return False
        
        if verbose:
            if "already exists" in result.stderr.lower():
                print(f"✓ Bucket already exists: {bucket_name}")
            else:
                print(f"✓ Bucket created: {bucket_name}")
        
        # Step 2: Set public read policy using AWS CLI
        if verbose:
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
        
        policy_json = json.dumps(policy)
        
        # Build environment with AWS credentials
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
            print(f"ERROR: Could not set public policy: {result.stderr}")
            return False
        
        if verbose:
            print(f"✓ Public read access enabled for bucket: {bucket_name}")
        
        # Step 3: Set CORS configuration if requested
        if set_cors:
            if verbose:
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
                env=env
            )
            
            if result.returncode != 0:
                print(f"ERROR: Could not set CORS: {result.stderr}")
                return False
            
            if verbose:
                print(f"✓ CORS configuration set for bucket: {bucket_name}")
        
        if verbose:
            print(f"\n✓ Bucket '{bucket_name}' is ready for public access")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to setup bucket: {e}")
        return False


def verify_bucket_config(bucket_name: str, endpoint: Optional[str] = None) -> dict:
    """
    Verify bucket configuration (policy and CORS).
    
    Args:
        bucket_name: Bucket name to verify
        endpoint: S3 endpoint URL (defaults to AWS_PUBLIC_ENDPOINT env var)
        
    Returns:
        Dict with verification results
    """
    if endpoint is None:
        endpoint = os.getenv("AWS_PUBLIC_ENDPOINT", "s3-west.nrp-nautilus.io")
    
    env = os.environ.copy()
    results = {
        "bucket": bucket_name,
        "policy": None,
        "cors": None,
        "errors": []
    }
    
    # Check policy
    result = subprocess.run(
        [
            "aws", "s3api", "get-bucket-policy",
            "--bucket", bucket_name,
            "--endpoint-url", f"https://{endpoint}"
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env
    )
    
    if result.returncode == 0:
        try:
            results["policy"] = json.loads(json.loads(result.stdout)["Policy"])
        except:
            results["errors"].append(f"Could not parse policy: {result.stdout}")
    else:
        results["errors"].append(f"Could not get policy: {result.stderr}")
    
    # Check CORS
    result = subprocess.run(
        [
            "aws", "s3api", "get-bucket-cors",
            "--bucket", bucket_name,
            "--endpoint-url", f"https://{endpoint}"
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env
    )
    
    if result.returncode == 0:
        try:
            results["cors"] = json.loads(result.stdout)
        except:
            results["errors"].append(f"Could not parse CORS: {result.stdout}")
    else:
        results["errors"].append(f"Could not get CORS: {result.stderr}")
    
    return results


def main():
    """CLI entry point for bucket setup."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Setup S3 bucket with public access and CORS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument("bucket", help="Bucket name to setup")
    parser.add_argument("--remote", default="nrp", help="Rclone remote name (default: nrp)")
    parser.add_argument("--endpoint", help="S3 endpoint URL (default: AWS_PUBLIC_ENDPOINT env var)")
    parser.add_argument("--no-cors", action="store_true", help="Skip CORS configuration")
    parser.add_argument("--verify", action="store_true", help="Verify bucket configuration")
    parser.add_argument("--quiet", action="store_true", help="Minimal output")
    
    args = parser.parse_args()
    
    if args.verify:
        results = verify_bucket_config(args.bucket, args.endpoint)
        print(json.dumps(results, indent=2))
        return 0 if not results["errors"] else 1
    
    success = setup_public_bucket(
        bucket_name=args.bucket,
        remote=args.remote,
        endpoint=args.endpoint,
        set_cors=not args.no_cors,
        verbose=not args.quiet
    )
    
    return 0 if success else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
