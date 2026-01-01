#!/usr/bin/env python3
"""
Convert vector datasets to optimized GeoParquet format.

Simple workflow:
1. Detect source CRS using GDAL
2. Read data with DuckDB ST_Read
3. Add ID column if needed
4. Reproject if needed (ST_Transform + ST_FlipCoordinates)
5. Write locally with geoparquet-io optimizations
6. Upload to S3 via rclone if needed
"""

import argparse
import sys
import tempfile
import os
import subprocess
from pathlib import Path
from typing import Optional, Tuple
import duckdb
import geopandas as gpd


def detect_crs(source_url: str, verbose: bool = False) -> Optional[str]:
    """
    Detect the CRS of a vector dataset using geopandas.
    
    Works with VSI paths (/vsicurl/, s3://, etc.).
    
    Args:
        source_url: Source dataset URL
        verbose: Print debug information
        
    Returns:
        CRS string (e.g., "EPSG:4326") or None if detection fails
    """
    try:
        # Read just the first row to get CRS info quickly
        gdf = gpd.read_file(source_url, rows=1)
        
        if gdf.crs is None:
            if verbose:
                print("Warning: No CRS found in dataset")
            return None
        
        # Try to get EPSG code
        if gdf.crs.to_epsg():
            return f"EPSG:{gdf.crs.to_epsg()}"
        
        # Fallback to authority string if available
        if gdf.crs.to_authority():
            auth, code = gdf.crs.to_authority()
            return f"{auth}:{code}"
        
        if verbose:
            print(f"Warning: Could not determine EPSG code, CRS is: {gdf.crs}")
        return None
        
    except Exception as e:
        if verbose:
            print(f"Warning: CRS detection failed: {e}")
        return None


def is_geographic_crs(crs: str) -> bool:
    """
    Check if a CRS is geographic (uses degrees) vs projected (uses meters).
    
    Args:
        crs: CRS string (e.g., "EPSG:4326")
        
    Returns:
        True if geographic, False if projected
    """
    # Common geographic CRS codes
    if crs in ["EPSG:4326", "EPSG:4269", "EPSG:4267"]:
        return True
    
    # Extract EPSG code
    if crs.startswith("EPSG:"):
        try:
            code = int(crs.split(":")[1])
            # EPSG codes 4000-4999 are typically geographic
            if 4000 <= code < 5000:
                return True
        except (ValueError, IndexError):
            pass
    
    return False


def check_id_column(source_url: str, id_column: Optional[str] = None, 
                    force_id: bool = True, verbose: bool = False) -> Tuple[bool, str]:
    """
    Check if we need to create an ID column.
    
    Args:
        source_url: Source dataset URL
        id_column: Specific ID column name to use
        force_id: Create _cng_fid if no suitable ID exists
        verbose: Print debug information
        
    Returns:
        (needs_id, id_column_name) tuple
    """
    con = duckdb.connect(':memory:')
    con.install_extension("spatial")
    con.load_extension("spatial")
    
    try:
        # Read just the schema - ST_Read handles URLs directly
        columns = con.execute(f"""
            DESCRIBE SELECT * FROM ST_Read('{source_url}') LIMIT 0
        """).fetchall()
        
        column_names = [col[0].lower() for col in columns]
        
        # If user specified an ID column, check if it exists
        if id_column:
            if id_column.lower() in column_names:
                return False, id_column  # Use existing column
            else:
                raise ValueError(f"Specified ID column '{id_column}' not found in source data")
        
        # Look for common ID column names
        common_ids = ['id', 'fid', 'objectid', 'gid', 'uid']
        for id_name in common_ids:
            if id_name in column_names:
                if verbose:
                    print(f"  Found existing ID column: {id_name}")
                return False, id_name
        
        # No ID found - create one if force_id is True
        if force_id:
            if verbose:
                print("  No ID column found - will create _cng_fid")
            return True, "_cng_fid"
        else:
            raise ValueError("No ID column found and force_id=False")
            
    finally:
        con.close()


def build_read_reproject_query(source_url: str, source_crs: Optional[str], 
                               target_crs: str, verbose: bool = False) -> str:
    """
    Build DuckDB query to read and reproject data. Nothing about IDs.
    
    Args:
        source_url: Source dataset URL
        source_crs: Source CRS (None = no reprojection needed)
        target_crs: Target CRS
        verbose: Print debug information
        
    Returns:
        DuckDB SQL query string
    """
    # Determine geometry transformation
    if source_crs and source_crs != target_crs:
        # Need to reproject
        if is_geographic_crs(target_crs):
            # ST_Transform outputs lat/lon for geographic CRS, but GeoParquet expects lon/lat
            geom_expr = f"ST_FlipCoordinates(ST_Transform(geom, '{source_crs}', '{target_crs}')) AS geom"
        else:
            geom_expr = f"ST_Transform(geom, '{source_crs}', '{target_crs}') AS geom"
    else:
        # No reprojection needed
        geom_expr = "geom"
    
    query = f"""
    SELECT 
        * EXCLUDE (geom),
        {geom_expr}
    FROM ST_Read('{source_url}')
    """
    
    if verbose:
        print(f"Read/Reproject Query: {query.strip()}")
    
    return query


def add_id_column_query(base_query: str, id_column: str = "_cng_fid") -> str:
    """
    Wrap a query to add a synthetic ID column.
    
    Args:
        base_query: Base query that reads/reprojects data
        id_column: Name of ID column to create
        
    Returns:
        Wrapped query with ID column
    """
    return f"""
    SELECT 
        ROW_NUMBER() OVER () AS {id_column},
        *
    FROM ({base_query})
    """


def write_with_duckdb(query: str, output_path: str,
                      compression: str = "ZSTD",
                      compression_level: int = 15,
                      row_group_size: int = 100000,
                      verbose: bool = False) -> None:
    """
    Write parquet file using DuckDB COPY. Simple and reliable.
    
    Args:
        query: DuckDB query that produces the data to write
        output_path: Local output file path
        compression: Compression algorithm
        compression_level: Compression level
        row_group_size: Rows per group
        verbose: Print debug information
    """
    con = duckdb.connect(':memory:')
    con.install_extension("spatial")
    con.load_extension("spatial")
    
    try:
        if verbose:
            print(f"  Writing with DuckDB to {output_path}...")
        
        # Use COPY - it works!
        con.execute(f"""
            COPY ({query})
            TO '{output_path}'
            (FORMAT PARQUET, 
             COMPRESSION {compression},
             ROW_GROUP_SIZE {row_group_size})
        """)
        
        if verbose:
            print(f"  ✓ Wrote {output_path}")
            
    finally:
        con.close()


def apply_geoparquet_optimizations(input_path: str, output_path: str,
                                    verbose: bool = False) -> None:
    """
    Apply geoparquet-io optimizations to an existing parquet file.
    
    This adds proper GeoParquet 1.1 metadata and optimizations.
    
    Args:
        input_path: Input parquet file
        output_path: Output parquet file (can be same as input for in-place)
        verbose: Print debug information
    """
    # Import the actual function from the module
    from geoparquet_io.core.add_bbox_column import add_bbox_column
    
    if verbose:
        print(f"  Applying GeoParquet optimizations...")
    
    # Add bbox column which also ensures proper metadata
    add_bbox_column(input_path, output_path, verbose=verbose)
    
    if verbose:
        print(f"  ✓ Applied optimizations")


def upload_to_s3(local_path: str, s3_destination: str, verbose: bool = True) -> None:
    """
    Upload a local file to S3 using rclone.
    
    Args:
        local_path: Local file path
        s3_destination: S3 destination (s3://bucket/path)
        verbose: Print progress information
    """
    # Convert s3://bucket/path to rclone format nrp:bucket/path
    if not s3_destination.startswith('s3://'):
        raise ValueError(f"S3 destination must start with s3://: {s3_destination}")
    
    # Parse s3://bucket/path -> nrp:bucket/path
    # The 'nrp' remote is configured in rclone for NRP Nautilus S3
    s3_path = s3_destination[5:]  # Remove s3://
    rclone_dest = f"nrp:{s3_path}"
    
    if verbose:
        print(f"  Uploading {local_path} to {s3_destination}...")
    
    result = subprocess.run(
        ['rclone', 'copyto', local_path, rclone_dest, '--progress'],
        capture_output=not verbose,
        text=True
    )
    
    if result.returncode != 0:
        raise RuntimeError(f"rclone upload failed: {result.stderr if result.stderr else 'Unknown error'}")
    
    if verbose:
        print(f"  ✓ Uploaded to {s3_destination}")


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
    target_crs: str = "EPSG:4326",
    verbose: bool = False
):
    """
    Convert a vector dataset to optimized GeoParquet.
    
    Workflow:
    1. Detect source CRS using GDAL
    2. Check if ID column exists or needs creation
    3. Build DuckDB query to read, add ID, and reproject
    4. Write GeoParquet locally with geoparquet-io optimizations
    5. Upload to S3 via rclone if destination is S3
    
    Args:
        source_url: Source dataset URL (supports s3://, http://, file paths)
        destination: Output path (s3:// or local file path)
        compression: Compression algorithm (ZSTD, GZIP, SNAPPY, NONE)
        compression_level: Compression level (1-22 for ZSTD)
        row_group_size: Number of rows per group
        geometry_encoding: Geometry encoding (WKB, WKT)
        id_column: Specific ID column to use (auto-detected if not specified)
        force_id: Create _cng_fid if no suitable ID column exists
        progress: Show progress during conversion
        target_crs: Target CRS for output (default: EPSG:4326)
        verbose: Print detailed debug information
    """
    print(f"Converting {source_url}")
    print(f"       to {destination}")
    
    if progress:
        print(f"  Compression: {compression} level {compression_level}")
        print(f"  Row group size: {row_group_size:,}")
    
    try:
        # Step 1: Detect source CRS
        print("  Detecting source CRS...")
        source_crs = detect_crs(source_url, verbose=verbose)
        
        needs_reprojection = False
        if source_crs:
            if source_crs != target_crs:
                print(f"  Source CRS: {source_crs} -> Reprojecting to {target_crs}")
                needs_reprojection = True
            else:
                print(f"  Source already in {target_crs}")
        else:
            print(f"  Warning: Could not detect source CRS, assuming {target_crs}")
            source_crs = None
        
        # Step 2: Build read/reproject query
        print("  Building read/reproject query...")
        query = build_read_reproject_query(
            source_url,
            source_crs if needs_reprojection else None,
            target_crs,
            verbose=verbose
        )
        
        # Step 3: Check ID column and wrap query if needed
        print("  Checking for ID column...")
        needs_id, id_col_name = check_id_column(source_url, id_column, force_id, verbose)
        
        if needs_id:
            print(f"  Adding synthetic ID column: {id_col_name}")
            query = add_id_column_query(query, id_col_name)
        else:
            print(f"  Using existing ID column: {id_col_name}")
        
        # Step 4: Write with DuckDB
        is_s3_dest = destination.startswith('s3://')
        
        if is_s3_dest:
            # Write to temp file first
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
                tmp_path = tmp.name
            
            try:
                # Write data
                write_with_duckdb(query, tmp_path, compression, compression_level, 
                                 row_group_size, verbose)
                
                # Apply GeoParquet optimizations in-place
                apply_geoparquet_optimizations(tmp_path, tmp_path, verbose)
                
                # Upload to S3
                upload_to_s3(tmp_path, destination, verbose=progress)
                
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
        else:
            # Write directly to destination
            write_with_duckdb(query, destination, compression, compression_level,
                            row_group_size, verbose)
            
            # Apply GeoParquet optimizations in-place
            apply_geoparquet_optimizations(destination, destination, verbose)
        
        print("✓ Conversion completed successfully!")
        
    except Exception as e:
        print(f"✗ Conversion failed: {e}", file=sys.stderr)
        if verbose:
            import traceback
            traceback.print_exc()
        raise


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Convert vector datasets to optimized GeoParquet format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert local shapefile
  cng-convert-to-parquet input.shp output.parquet
  
  # Convert from S3 to S3
  cng-convert-to-parquet s3://bucket/input.shp s3://bucket/output.parquet
  
  # Convert with custom compression
  cng-convert-to-parquet input.shp output.parquet --compression GZIP --compression-level 9
  
  # Convert and specify ID column
  cng-convert-to-parquet input.shp output.parquet --id-column objectid
        """
    )
    
    parser.add_argument("source", help="Source dataset URL or path")
    parser.add_argument("destination", help="Output GeoParquet path")
    
    parser.add_argument("--compression", default="ZSTD",
                       choices=["ZSTD", "GZIP", "SNAPPY", "NONE"],
                       help="Compression algorithm (default: ZSTD)")
    parser.add_argument("--compression-level", type=int, default=15,
                       help="Compression level (default: 15 for ZSTD)")
    parser.add_argument("--row-group-size", type=int, default=100000,
                       help="Rows per group (default: 100000)")
    parser.add_argument("--geometry-encoding", default="WKB",
                       choices=["WKB", "WKT"],
                       help="Geometry encoding (default: WKB)")
    
    parser.add_argument("--id-column", help="ID column name (auto-detected if not specified)")
    parser.add_argument("--no-force-id", action="store_true",
                       help="Don't create synthetic ID if none exists")
    parser.add_argument("--target-crs", default="EPSG:4326",
                       help="Target CRS (default: EPSG:4326)")
    
    parser.add_argument("--no-progress", action="store_true",
                       help="Disable progress output")
    parser.add_argument("--verbose", action="store_true",
                       help="Enable detailed debug output")
    
    args = parser.parse_args()
    
    try:
        convert_to_parquet(
            source_url=args.source,
            destination=args.destination,
            compression=args.compression,
            compression_level=args.compression_level,
            row_group_size=args.row_group_size,
            geometry_encoding=args.geometry_encoding,
            id_column=args.id_column,
            force_id=not args.no_force_id,
            progress=not args.no_progress,
            target_crs=args.target_crs,
            verbose=args.verbose
        )
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
