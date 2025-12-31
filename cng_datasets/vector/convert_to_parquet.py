#!/usr/bin/env python3
"""
Convert vector datasets to optimized GeoParquet format.

This script provides a DuckDB-based conversion with guaranteed ID columns
and cloud-native optimizations for large-scale dataset processing.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional, Tuple
import duckdb


def convert_to_parquet(
    source_url: str,
    destination: str,
    bucket: Optional[str] = None,
    create_bucket: bool = False,
    compression: str = "ZSTD",
    compression_level: int = 15,
    row_group_size: int = 100000,
    geometry_encoding: str = "WKB",
    id_column: Optional[str] = None,
    force_id: bool = True,
    progress: bool = True
):
    """
    Convert a vector dataset to optimized GeoParquet with guaranteed ID column.
    
    Uses DuckDB spatial extension for larger-than-RAM processing with streaming.
    Ensures every output has a valid, unique ID column for downstream processing.
    
    Args:
        source_url: Source dataset URL (supports /vsicurl/, s3://, file paths)
        destination: Output path (supports /vsis3/, file paths)
        bucket: S3 bucket name (required if create_bucket=True)
        create_bucket: Whether to create and configure the bucket
        compression: Compression algorithm (ZSTD, GZIP, SNAPPY, NONE)
        compression_level: Compression level (1-22 for ZSTD)
        row_group_size: Number of rows per group (affects query performance)
        geometry_encoding: Geometry encoding (WKB, WKT)
        id_column: Specific ID column to use (auto-detected if not specified)
        force_id: Create _cng_fid if no suitable ID column exists
        progress: Show progress during conversion
    """
    # Create bucket if requested
    if create_bucket:
        if not bucket:
            raise ValueError("bucket parameter required when create_bucket=True")
        
        print(f"Creating bucket: {bucket}")
        from cng_datasets.storage.rclone import create_public_bucket
        create_public_bucket(bucket, remote='nrp', set_cors=True)
    
    # Prepare source path for DuckDB
    if source_url.startswith('http://') or source_url.startswith('https://'):
        source_path = f"/vsicurl/{source_url}"
    else:
        source_path = source_url
    
    print(f"Converting {source_url} to {destination}")
    if progress:
        print(f"  Compression: {compression} level {compression_level}")
        print(f"  Row group size: {row_group_size:,}")
    
    # Setup DuckDB connection
    con = _setup_duckdb_connection()
    
    try:
        # Configure S3 credentials for DuckDB
        from cng_datasets.storage.s3 import configure_s3_credentials
        configure_s3_credentials(con)
        
        # Read source and detect ID column
        print("  Reading source data...")
        id_col, has_valid_id = _identify_or_create_id_column(
            con, source_path, id_column, force_id, progress
        )
        
        # Build query with ID column handling
        query = _build_conversion_query(source_path, id_col, has_valid_id)
        
        # Write optimized GeoParquet
        print(f"  Writing optimized GeoParquet (ID column: {id_col})...")
        _write_geoparquet(
            con, query, destination, compression, compression_level,
            row_group_size, geometry_encoding, progress
        )
        
        print("✓ Conversion completed successfully!")
        if id_col and not has_valid_id:
            print(f"  Note: Created synthetic ID column '{id_col}' for downstream processing")
        
    except Exception as e:
        print(f"✗ Conversion failed: {e}", file=sys.stderr)
        raise
    finally:
        con.close()


def _setup_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Setup DuckDB with spatial extension."""
    import os
    
    # Configure GDAL SSL certificate path for DuckDB spatial extension
    # Try common certificate bundle locations
    cert_paths = [
        '/etc/ssl/certs/ca-certificates.crt',  # Debian/Ubuntu
        '/etc/ssl/certs/ca-bundle.crt',         # RedHat/CentOS
        '/etc/pki/tls/certs/ca-bundle.crt',    # Older RedHat
        '/etc/ssl/cert.pem',                    # Alpine
        '/usr/local/share/ca-certificates/',   # Custom installs
    ]
    
    for cert_path in cert_paths:
        if os.path.exists(cert_path):
            os.environ['CURL_CA_BUNDLE'] = cert_path
            os.environ['SSL_CERT_FILE'] = cert_path
            break
    else:
        # If no cert bundle found, disable SSL verification as fallback
        # (not ideal but necessary for some container environments)
        os.environ['GDAL_HTTP_UNSAFESSL'] = 'YES'
    
    con = duckdb.connect()
    
    # Install and load spatial extension (uses GDAL internally)
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    
    # Configure for large files
    con.execute("SET temp_directory='/tmp'")
    con.execute("SET http_retries=20")
    con.execute("SET http_retry_wait_ms=5000")
    
    return con


def _identify_or_create_id_column(
    con: duckdb.DuckDBPyConnection,
    source_path: str,
    specified_id: Optional[str],
    force_id: bool,
    verbose: bool
) -> Tuple[str, bool]:
    """
    Identify existing ID column or determine if synthetic ID needed.
    
    Returns:
        Tuple of (id_column_name, has_existing_valid_id)
    """
    from cng_datasets.vector.h3_tiling import identify_id_column
    
    # Create temporary view to inspect source
    con.execute(f"""
        CREATE OR REPLACE VIEW source_data AS 
        SELECT * FROM ST_Read('{source_path}')
    """)
    
    # Use the existing identify_id_column logic
    id_col, is_unique = identify_id_column(
        con, 'source_data', 
        specified_id_col=specified_id,
        check_uniqueness=True
    )
    
    if id_col and is_unique:
        if verbose:
            print(f"  Using existing ID column: {id_col}")
        return id_col, True
    elif id_col and not is_unique:
        if verbose:
            print(f"  Warning: ID column '{id_col}' is not unique")
        if force_id:
            if verbose:
                print(f"  Creating synthetic ID column: _cng_fid")
            return '_cng_fid', False
        return id_col, False
    else:
        # No ID column found
        if force_id:
            if verbose:
                print(f"  No ID column found, creating: _cng_fid")
            return '_cng_fid', False
        return None, False


def _build_conversion_query(
    source_path: str, 
    id_col: Optional[str],
    has_existing_id: bool
) -> str:
    """Build SQL query for conversion with ID column handling."""
    
    if id_col and not has_existing_id:
        # Need to create synthetic ID
        query = f"""
            SELECT 
                row_number() OVER () AS {id_col},
                *
            FROM ST_Read('{source_path}')
        """
    else:
        # Use existing columns as-is
        query = f"SELECT * FROM ST_Read('{source_path}')"
    
    return query


def _write_geoparquet(
    con: duckdb.DuckDBPyConnection,
    query: str,
    destination: str,
    compression: str,
    compression_level: int,
    row_group_size: int,
    geometry_encoding: str,
    verbose: bool
):
    """Write query results to optimized GeoParquet file."""
    
    # DuckDB COPY command for optimized writes
    copy_sql = f"""
        COPY ({query}) 
        TO '{destination}' 
        (
            FORMAT PARQUET,
            COMPRESSION '{compression}',
            COMPRESSION_LEVEL {compression_level},
            ROW_GROUP_SIZE {row_group_size}
        )
    """
    
    if verbose:
        print(f"  Executing: COPY (...) TO '{destination}'")
    
    con.execute(copy_sql)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Convert vector datasets to optimized GeoParquet format",
        epilog="Uses DuckDB for larger-than-RAM processing and ensures valid ID columns."
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
        "--compression-level",
        type=int,
        default=15,
        help="Compression level (default: 15 for ZSTD, 1-22)"
    )
    parser.add_argument(
        "--row-group-size",
        type=int,
        default=100000,
        help="Rows per group, affects query performance (default: 100,000)"
    )
    parser.add_argument(
        "--geometry-encoding",
        default="WKB",
        choices=["WKB", "WKT"],
        help="Geometry encoding (default: WKB)"
    )
    parser.add_argument(
        "--id-column",
        help="Specific ID column to use (auto-detected if not specified)"
    )
    parser.add_argument(
        "--no-force-id",
        action="store_true",
        help="Don't create synthetic _cng_fid if no valid ID column exists"
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Don't show progress messages"
    )
    
    args = parser.parse_args()
    
    convert_to_parquet(
        source_url=args.source_url,
        destination=args.destination,
        bucket=args.bucket,
        create_bucket=args.create_bucket,
        compression=args.compression,
        compression_level=args.compression_level,
        row_group_size=args.row_group_size,
        geometry_encoding=args.geometry_encoding,
        id_column=args.id_column,
        force_id=not args.no_force_id,
        progress=not args.no_progress
    )


if __name__ == "__main__":
    main()
