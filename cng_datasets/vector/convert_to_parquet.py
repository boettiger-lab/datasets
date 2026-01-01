#!/usr/bin/env python3
"""
Convert vector datasets to optimized GeoParquet format.

This script provides conversion with guaranteed ID columns and proper GeoParquet
metadata using geoparquet-io for cloud-native optimizations.
"""

import argparse
import sys
import tempfile
import os
from pathlib import Path
from typing import Optional, Tuple
import duckdb


def convert_to_parquet(
    source_url: str,
    destination: str,
    compression: str = "ZSTD",
    compression_level: int = 15,
    row_group_size: int = 100000,
    geometry_encoding: str = "WKB",
    id_column: Optional[str] = None,
    force_id: bool = True,
    progress: bool = True,
    target_crs: str = "EPSG:4326"
):
    """
    Convert a vector dataset to optimized GeoParquet with guaranteed ID column.
    
    Uses geoparquet-io for proper GeoParquet 1.1 metadata and DuckDB for ID handling.
    Ensures every output has a valid, unique ID column for downstream processing.
    Automatically reprojects to EPSG:4326 if source is in a different CRS.
    
    Note: If writing to S3, ensure the bucket exists and has proper permissions.
    Use 'cng-datasets storage setup-bucket' before running this command.
    
    Args:
        source_url: Source dataset URL (supports /vsicurl/, s3://, file paths)
        destination: Output path (supports /vsis3/, s3://, file paths)
        compression: Compression algorithm (ZSTD, GZIP, SNAPPY, NONE)
        compression_level: Compression level (1-22 for ZSTD)
        row_group_size: Number of rows per group (affects query performance)
        geometry_encoding: Geometry encoding (WKB, WKT)
        id_column: Specific ID column to use (auto-detected if not specified)
        force_id: Create _cng_fid if no suitable ID column exists
        progress: Show progress during conversion
        target_crs: Target CRS for output (default: EPSG:4326 for H3 hex processing)
    """
    # Convert destination path: /vsis3/bucket/path -> s3://bucket/path
    dest_path = destination
    if destination.startswith('/vsis3/'):
        dest_path = 's3://' + destination[7:]
    elif destination.startswith('s3://'):
        dest_path = destination
    
    print(f"Converting {source_url} to {dest_path}")
    if progress:
        print(f"  Compression: {compression} level {compression_level}")
        print(f"  Row group size: {row_group_size:,}")
    
    try:
        # Check projection and reproject if necessary
        print("  Checking projection...")
        needs_reprojection, source_crs = _check_projection(source_url, target_crs)
        if needs_reprojection:
            print(f"  Source CRS: {source_crs} -> Reprojecting to {target_crs}")
        else:
            print(f"  Source already in {target_crs}")
        
        # Check if we need to add an ID column
        print("  Checking for ID column...")
        needs_id, id_col_name = _check_needs_id_column(source_url, id_column, force_id)
        
        if needs_id:
            # Two-step process: convert with geoparquet-io, then add ID column
            print(f"  Creating synthetic ID column: {id_col_name}")
            _convert_with_id_column(source_url, dest_path, id_col_name, compression, 
                                   compression_level, row_group_size, progress,
                                   needs_reprojection, target_crs)
        else:
            # Direct conversion with geoparquet-io
            print(f"  Using existing ID column: {id_col_name}")
            _convert_direct(source_url, dest_path, compression, compression_level, 
                           row_group_size, progress, needs_reprojection, target_crs)
        
        print("✓ Conversion completed successfully!")
        if needs_id:
            print(f"  Note: Created synthetic ID column '{id_col_name}' for downstream processing")
        
    except Exception as e:
        print(f"✗ Conversion failed: {e}", file=sys.stderr)
        raise


def _check_projection(
    source_url: str,
    target_crs: str = "EPSG:4326"
) -> Tuple[bool, Optional[str]]:
    """
    Check if source dataset needs reprojection.
    
    Returns:
        Tuple of (needs_reprojection, source_crs)
    """
    import subprocess
    import json
    
    _configure_ssl()
    
    # Prepare source path for GDAL
    if source_url.startswith('http://') or source_url.startswith('https://'):
        source_path = f"/vsicurl/{source_url}"
    else:
        source_path = source_url
    
    try:
        # Use ogrinfo to get CRS information in JSON format
        result = subprocess.run(
            ['ogrinfo', '-json', source_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            print(f"  Warning: Could not read source metadata: {result.stderr}")
            return False, None
        
        # Parse JSON output
        info = json.loads(result.stdout)
        
        # Extract CRS from first layer
        if 'layers' in info and len(info['layers']) > 0:
            layer = info['layers'][0]
            if 'geometryFields' in layer and len(layer['geometryFields']) > 0:
                geom_field = layer['geometryFields'][0]
                if 'coordinateSystem' in geom_field:
                    crs_info = geom_field['coordinateSystem']
                    
                    # Try to get EPSG code from various locations
                    source_crs = None
                    
                    # Check for authority name and code
                    if 'id' in crs_info:
                        auth = crs_info['id'].get('authority', '')
                        code = crs_info['id'].get('code', '')
                        if auth and code:
                            source_crs = f"{auth}:{code}"
                    
                    # Fallback: check WKT for EPSG reference
                    if not source_crs and 'wkt' in crs_info:
                        wkt = crs_info['wkt']
                        if 'EPSG",3310' in wkt or 'EPSG","3310' in wkt:
                            source_crs = "EPSG:3310"
                        elif 'EPSG",4269' in wkt or 'EPSG","4269' in wkt:
                            # NAD83 geographic
                            source_crs = "EPSG:4269"
                        elif 'EPSG",4326' in wkt or 'EPSG","4326' in wkt:
                            source_crs = "EPSG:4326"
                    
                    if source_crs:
                        # Normalize comparison
                        needs_reprojection = source_crs.upper() != target_crs.upper()
                        # Also check if source is NAD83 (EPSG:4269) and target is WGS84 (EPSG:4326)
                        # These are very similar but technically different
                        if source_crs.upper() == "EPSG:4269" and target_crs.upper() == "EPSG:4326":
                            # For most purposes, these are close enough, but let's reproject anyway
                            needs_reprojection = True
                        
                        return needs_reprojection, source_crs
        
        print("  Warning: No CRS found in source data, assuming EPSG:4326")
        return False, None
        
    except subprocess.TimeoutExpired:
        print("  Warning: Timeout reading source metadata, assuming EPSG:4326")
        return False, None
    except json.JSONDecodeError as e:
        print(f"  Warning: Could not parse source metadata: {e}, assuming EPSG:4326")
        return False, None
    except Exception as e:
        print(f"  Warning: Error checking projection: {e}, assuming EPSG:4326")
        return False, None


def _check_needs_id_column(
    source_url: str,
    specified_id: Optional[str],
    force_id: bool
) -> Tuple[bool, str]:
    """
    Check if source needs an ID column added.
    
    Returns:
        Tuple of (needs_id, id_column_name)
    """
    from cng_datasets.vector.h3_tiling import identify_id_column, setup_duckdb_connection
    
    # Configure SSL for GDAL
    _configure_ssl()
    
    # Setup DuckDB to inspect source
    con = setup_duckdb_connection()
    
    try:
        # Configure S3 credentials
        from cng_datasets.storage.s3 import configure_s3_credentials
        configure_s3_credentials(con)
        
        # Prepare source path
        if source_url.startswith('http://') or source_url.startswith('https://'):
            source_path = f"/vsicurl/{source_url}"
        else:
            source_path = source_url
        
        # Create view to inspect
        con.execute(f"""
            CREATE OR REPLACE VIEW source_data AS 
            SELECT * FROM ST_Read('{source_path}')
        """)
        
        # Check for ID column
        id_col, is_unique = identify_id_column(
            con, 'source_data', 
            specified_id_col=specified_id,
            check_uniqueness=True
        )
        
        if id_col and is_unique:
            return False, id_col  # Has valid ID, no need to add
        elif force_id:
            return True, '_cng_fid'  # Need to add synthetic ID
        else:
            return False, id_col if id_col else None
            
    finally:
        con.close()


def _configure_ssl():
    """Configure SSL certificates for GDAL."""
    cert_paths = [
        '/etc/ssl/certs/ca-certificates.crt',
        '/etc/ssl/certs/ca-bundle.crt',
        '/etc/pki/tls/certs/ca-bundle.crt',
        '/etc/ssl/cert.pem',
    ]
    
    for cert_path in cert_paths:
        if os.path.exists(cert_path):
            os.environ['CURL_CA_BUNDLE'] = cert_path
            os.environ['SSL_CERT_FILE'] = cert_path
            return
    
    # Fallback: disable SSL verification
    os.environ['GDAL_HTTP_UNSAFESSL'] = 'YES'


def _upload_via_rclone(local_file: str, destination: str, verbose: bool = True):
    """Upload file to S3 via rclone to preserve GeoParquet metadata."""
    import subprocess
    
    # Convert s3:// or /vsis3/ to rclone format: nrp:bucket/path
    if destination.startswith('s3://'):
        s3_path = destination[5:]  # Remove 's3://'
    elif destination.startswith('/vsis3/'):
        s3_path = destination[7:]  # Remove '/vsis3/'
    else:
        raise ValueError(f"Invalid S3 destination: {destination}")
    
    # Split bucket and path
    parts = s3_path.split('/', 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ''
    
    # Use rclone copyto for single file upload
    rclone_dest = f"nrp:{bucket}/{key}"
    
    cmd = ['rclone', 'copyto', local_file, rclone_dest]
    if verbose:
        cmd.append('-v')
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise RuntimeError(f"rclone upload failed: {result.stderr}")
    
    if verbose and result.stdout:
        print(result.stdout)


def _convert_direct(
    source_url: str,
    destination: str,
    compression: str,
    compression_level: int,
    row_group_size: int,
    verbose: bool,
    needs_reprojection: bool = False,
    target_crs: str = "EPSG:4326"
):
    """Convert directly using geoparquet-io or DuckDB if reprojection needed."""
    # If reprojection is needed, use DuckDB path
    if needs_reprojection:
        _convert_direct_with_reprojection(
            source_url, destination, compression, compression_level,
            row_group_size, verbose, target_crs
        )
        return
    
    # Otherwise use geoparquet-io for direct conversion
    try:
        from geoparquet_io.core.convert import convert_to_geoparquet
    except ImportError:
        raise ImportError("geoparquet-io is required. Install with: pip install geoparquet-io")
    
    # If destination is S3, write to temp file first, then upload via rclone
    # This preserves GeoParquet metadata (DuckDB COPY would strip it)
    if destination.startswith('s3://') or destination.startswith('/vsis3/'):
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            # Convert to local temp file with proper GeoParquet metadata
            if verbose:
                print(f"  Converting to temporary file: {tmp_path}")
            
            convert_to_geoparquet(
                input_file=source_url,
                output_file=tmp_path,
                skip_hilbert=True,
                verbose=verbose,
                compression=compression,
                compression_level=compression_level,
                row_group_rows=row_group_size,
                profile=None,
                geoparquet_version="1.1"
            )
            
            # Upload to S3 using rclone (preserves GeoParquet metadata)
            if verbose:
                print(f"  Uploading to S3 via rclone: {destination}")
            
            _upload_via_rclone(tmp_path, destination, verbose=verbose)
            
        finally:
            # Clean up temp file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    else:
        # Local file - convert directly
        convert_to_geoparquet(
            input_file=source_url,
            output_file=destination,
            skip_hilbert=True,
            verbose=verbose,
            compression=compression,
            compression_level=compression_level,
            row_group_rows=row_group_size,
            profile=None,
            geoparquet_version="1.1"
        )


def _convert_direct_with_reprojection(
    source_url: str,
    destination: str,
    compression: str,
    compression_level: int,
    row_group_size: int,
    verbose: bool,
    target_crs: str
):
    """Convert with reprojection using DuckDB + geoparquet-io write."""
    try:
        from geoparquet_io.core.common import write_parquet_with_metadata
    except ImportError:
        raise ImportError("geoparquet-io is required. Install with: pip install geoparquet-io")
    
    from cng_datasets.vector.h3_tiling import setup_duckdb_connection
    from cng_datasets.storage.s3 import configure_s3_credentials
    
    _configure_ssl()
    con = setup_duckdb_connection()
    
    try:
        configure_s3_credentials(con)
        
        # Prepare source path
        if source_url.startswith('http://') or source_url.startswith('https://'):
            source_path = f"/vsicurl/{source_url}"
        else:
            source_path = source_url
        
        # Read source and reproject
        if verbose:
            print(f"  Reading source and reprojecting to {target_crs}...")
        
        # Get source CRS for ST_Transform
        meta_result = con.execute(f"""
            SELECT
                layers[1].geometry_fields[1].crs.auth_name as auth_name,
                layers[1].geometry_fields[1].crs.auth_code as auth_code
            FROM ST_Read_Meta('{source_path}')
        """).fetchone()
        
        if meta_result and meta_result[0] and meta_result[1]:
            source_crs = f"{meta_result[0]}:{meta_result[1]}"
        else:
            source_crs = "EPSG:4326"  # fallback
        
        query = f"""
            SELECT 
                * EXCLUDE (geom),
                ST_FlipCoordinates(ST_Transform(geom, '{source_crs}', '{target_crs}')) as geom
            FROM ST_Read('{source_path}')
        """
        
        # If destination is S3, write to temp file first, then upload via rclone
        if destination.startswith('s3://') or destination.startswith('/vsis3/'):
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                tmp_path = tmp.name
            
            try:
                if verbose:
                    print(f"  Writing to temporary file: {tmp_path}")
                
                write_parquet_with_metadata(
                    con=con,
                    query=query,
                    output_file=tmp_path,
                    compression=compression,
                    compression_level=compression_level,
                    row_group_rows=row_group_size,
                    geoparquet_version="1.1",
                    verbose=verbose,
                    profile=None
                )
                
                if verbose:
                    print(f"  Uploading to S3 via rclone: {destination}")
                
                _upload_via_rclone(tmp_path, destination, verbose=verbose)
                
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
        else:
            # Local file - write directly
            if verbose:
                print(f"  Writing GeoParquet with proper metadata...")
            
            write_parquet_with_metadata(
                con=con,
                query=query,
                output_file=destination,
                compression=compression,
                compression_level=compression_level,
                row_group_rows=row_group_size,
                geoparquet_version="1.1",
                verbose=verbose,
                profile=None
            )
        
    finally:
        con.close()


def _convert_with_id_column(
    source_url: str,
    destination: str,
    id_col_name: str,
    compression: str,
    compression_level: int,
    row_group_size: int,
    verbose: bool,
    needs_reprojection: bool = False,
    target_crs: str = "EPSG:4326"
):
    """Convert with synthetic ID column using DuckDB + geoparquet-io write."""
    try:
        from geoparquet_io.core.common import write_parquet_with_metadata
    except ImportError:
        raise ImportError("geoparquet-io is required. Install with: pip install geoparquet-io")
    
    from cng_datasets.vector.h3_tiling import setup_duckdb_connection
    from cng_datasets.storage.s3 import configure_s3_credentials
    
    _configure_ssl()
    
    con = setup_duckdb_connection()
    
    try:
        configure_s3_credentials(con)
        
        # Prepare source path
        if source_url.startswith('http://') or source_url.startswith('https://'):
            source_path = f"/vsicurl/{source_url}"
        else:
            source_path = source_url
        
        # Read source with DuckDB spatial and add ID column
        if verbose:
            action = "Reading source, reprojecting, and adding ID column" if needs_reprojection else "Reading source and adding ID column"
            print(f"  {action}...")
        
        # Build query with optional reprojection
        if needs_reprojection:
            # Get source CRS for ST_Transform
            meta_result = con.execute(f"""
                SELECT
                    layers[1].geometry_fields[1].crs.auth_name as auth_name,
                    layers[1].geometry_fields[1].crs.auth_code as auth_code
                FROM ST_Read_Meta('{source_path}')
            """).fetchone()
            
            if meta_result and meta_result[0] and meta_result[1]:
                source_crs = f"{meta_result[0]}:{meta_result[1]}"
            else:
                source_crs = "EPSG:4326"  # fallback
            
            query = f"""
                SELECT 
                    row_number() OVER () AS {id_col_name},
                    * EXCLUDE (geom),
                    ST_FlipCoordinates(ST_Transform(geom, '{source_crs}', '{target_crs}')) as geom
                FROM ST_Read('{source_path}')
            """
        else:
            query = f"""
                SELECT 
                    row_number() OVER () AS {id_col_name},
                    * EXCLUDE (geom),
                    geom
                FROM ST_Read('{source_path}')
            """
        
        # If destination is S3, write to temp file first, then upload via rclone
        # This preserves GeoParquet metadata (DuckDB COPY would strip it)
        if destination.startswith('s3://') or destination.startswith('/vsis3/'):
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                tmp_path = tmp.name
            
            try:
                # Write to local temp file with proper GeoParquet metadata
                if verbose:
                    print(f"  Writing to temporary file: {tmp_path}")
                
                write_parquet_with_metadata(
                    con=con,
                    query=query,
                    output_file=tmp_path,
                    compression=compression,
                    compression_level=compression_level,
                    row_group_rows=row_group_size,
                    geoparquet_version="1.1",
                    verbose=verbose,
                    profile=None
                )
                
                # Upload to S3 using rclone (preserves GeoParquet metadata)
                if verbose:
                    print(f"  Uploading to S3 via rclone: {destination}")
                
                _upload_via_rclone(tmp_path, destination, verbose=verbose)
                
            finally:
                # Clean up temp file
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
        else:
            # Local file - write directly
            if verbose:
                print(f"  Writing GeoParquet with proper metadata...")
            
            write_parquet_with_metadata(
                con=con,
                query=query,
                output_file=destination,
                compression=compression,
                compression_level=compression_level,
                row_group_rows=row_group_size,
                geoparquet_version="1.1",
                verbose=verbose,
                profile=None
            )
        
    finally:
        con.close()


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
