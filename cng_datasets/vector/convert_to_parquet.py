#!/usr/bin/env python3
"""
Convert vector datasets to optimized GeoParquet format.

This script provides a simple CLI interface for converting vector datasets
to GeoParquet with sensible optimizations for cloud-native access.
"""

import argparse
import subprocess
import sys
from pathlib import Path


def convert_to_parquet(
    source_url: str,
    destination: str,
    bucket: str = None,
    create_bucket: bool = False,
    compression: str = "ZSTD",
    row_group_size: int = 65536,
    preserve_fid: bool = True,
    geometry_encoding: str = "WKB",
    progress: bool = True
):
    """
    Convert a vector dataset to GeoParquet with optimizations.
    
    Args:
        source_url: Source dataset URL (supports /vsicurl/, s3://, file paths)
        destination: Output path (supports /vsis3/, file paths)
        bucket: S3 bucket name (required if create_bucket=True)
        create_bucket: Whether to create and configure the bucket
        compression: Compression algorithm (ZSTD, GZIP, SNAPPY, NONE)
        row_group_size: Number of rows per group (affects query performance)
        preserve_fid: Whether to preserve feature IDs
        geometry_encoding: Geometry encoding (WKB, WKT)
        progress: Show progress during conversion
    """
    # Create bucket if requested
    if create_bucket:
        if not bucket:
            raise ValueError("bucket parameter required when create_bucket=True")
        
        print(f"Creating bucket: {bucket}")
        from cng_datasets.storage.rclone import create_public_bucket
        create_public_bucket(bucket, remote='nrp', set_cors=True)
    
    # Prepare source path
    if source_url.startswith('http://') or source_url.startswith('https://'):
        source_path = f"/vsicurl/{source_url}"
    else:
        source_path = source_url
    
    # Build ogr2ogr command
    cmd = [
        "ogr2ogr",
        "-f", "Parquet",
        destination,
        source_path,
        "-lco", f"COMPRESSION={compression}",
        "-lco", f"ROW_GROUP_SIZE={row_group_size}",
        "-lco", f"GEOMETRY_ENCODING={geometry_encoding}"
    ]
    
    if preserve_fid:
        cmd.extend(["-lco", "FID="])
    
    if progress:
        cmd.append("-progress")
    
    # Execute conversion
    print(f"Converting {source_url} to {destination}")
    print(f"Command: {' '.join(cmd)}")
    
    try:
        subprocess.run(cmd, check=True)
        print("Conversion completed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"Conversion failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(1)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Convert vector datasets to optimized GeoParquet format"
    )
    parser.add_argument(
        "source_url",
        help="Source dataset URL (http://, https://, s3://, or file path)"
    )
    parser.add_argument(
        "destination",
        help="Output GeoParquet path (file path or /vsis3/bucket/path)"
    )
    parser.add_argument(
        "--bucket",
        help="S3 bucket name (required with --create-bucket)"
    )
    parser.add_argument(
        "--create-bucket",
        action="store_true",
        help="Create and configure S3 bucket before conversion"
    )
    parser.add_argument(
        "--compression",
        default="ZSTD",
        choices=["ZSTD", "GZIP", "SNAPPY", "NONE"],
        help="Compression algorithm (default: ZSTD)"
    )
    parser.add_argument(
        "--row-group-size",
        type=int,
        default=65536,
        help="Rows per group, affects query performance (default: 65536)"
    )
    parser.add_argument(
        "--no-preserve-fid",
        action="store_true",
        help="Don't preserve feature IDs"
    )
    parser.add_argument(
        "--geometry-encoding",
        default="WKB",
        choices=["WKB", "WKT"],
        help="Geometry encoding (default: WKB)"
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Don't show progress"
    )
    
    args = parser.parse_args()
    
    convert_to_parquet(
        source_url=args.source_url,
        destination=args.destination,
        bucket=args.bucket,
        create_bucket=args.create_bucket,
        compression=args.compression,
        row_group_size=args.row_group_size,
        preserve_fid=not args.no_preserve_fid,
        geometry_encoding=args.geometry_encoding,
        progress=not args.no_progress
    )


if __name__ == "__main__":
    main()
