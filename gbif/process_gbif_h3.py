"""
Process GBIF parquet data from S3, add H3 geospatial indexes (h0-h10), 
and write back to S3 partitioned by h0.

This script:
1. Reads parquet files from s3://gbif-open-data-us-east-1/occurrence/2025-06-01/occurrence.parquet/
2. Adds H3 index columns (h0 through h10) based on decimallongitude and decimallatitude
3. Writes results to s3://public-gbif/2025-06/chunks partitioned by h0
4. Appends to existing partitions to avoid overwriting
"""

import duckdb
import os
from pathlib import Path

def setup_duckdb():
    """Initialize DuckDB connection with required extensions."""
    con = duckdb.connect()
    
    # Install and load required extensions
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("INSTALL h3 FROM community;")
    con.execute("LOAD h3;")
    
    # Configure AWS credentials for source bucket (standard AWS)
    con.execute("SET s3_region='us-east-1';")
    
    # Configure DuckDB secret for writing to public-gbif bucket
    # This uses the custom S3 endpoint from the k8s environment
    aws_key = os.environ.get('AWS_ACCESS_KEY_ID', '')
    aws_secret = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
    s3_endpoint = os.environ.get('AWS_S3_ENDPOINT', 'rook-ceph-rgw-nautiluss3.rook')
    use_ssl = os.environ.get('AWS_HTTPS', 'false').lower() == 'true'
    
    # Create secret for public-gbif bucket
    con.execute(f"""
        CREATE SECRET IF NOT EXISTS public_gbif (
            TYPE S3,
            KEY_ID '{aws_key}',
            SECRET '{aws_secret}',
            REGION 'us-east-1',
            ENDPOINT '{s3_endpoint}',
            USE_SSL {use_ssl},
            URL_STYLE 'path',
            SCOPE 's3://public-gbif'
        );
    """)
    
    print(f"  ✓ Configured S3 secret for public-gbif bucket (endpoint: {s3_endpoint})")
    
    return con

def get_source_files(con, source_path):
    """List all parquet files in the source S3 bucket."""
    query = f"""
    SELECT DISTINCT filename 
    FROM read_parquet('{source_path}/*', filename=true)
    LIMIT 1
    """
    try:
        # First check if we can access the bucket
        con.execute(f"SELECT COUNT(*) FROM read_parquet('{source_path}/*') LIMIT 1")
        return source_path
    except Exception as e:
        print(f"Error accessing source: {e}")
        return None

def process_gbif_chunk(con, source_file, output_base):
    """
    Process a single GBIF parquet file and write to output bucket partitioned by h0.
    
    Args:
        con: DuckDB connection
        source_file: Path to source parquet file(s) 
        output_base: Base path for output (s3://public-gbif/2025-06/chunks)
    """
    
    print(f"Processing files from: {source_file}")
    
    # Query to add H3 columns and partition by h0
    # We'll process each h0 partition separately to avoid overwriting
    query = """
    WITH data_with_h3 AS (
        SELECT 
            *,
            h3_latlng_to_cell(decimallatitude, decimallongitude, 0) AS h0,
            h3_latlng_to_cell(decimallatitude, decimallongitude, 1) AS h1,
            h3_latlng_to_cell(decimallatitude, decimallongitude, 2) AS h2,
            h3_latlng_to_cell(decimallatitude, decimallongitude, 3) AS h3,
            h3_latlng_to_cell(decimallatitude, decimallongitude, 4) AS h4,
            h3_latlng_to_cell(decimallatitude, decimallongitude, 5) AS h5,
            h3_latlng_to_cell(decimallatitude, decimallongitude, 6) AS h6,
            h3_latlng_to_cell(decimallatitude, decimallongitude, 7) AS h7,
            h3_latlng_to_cell(decimallatitude, decimallongitude, 8) AS h8,
            h3_latlng_to_cell(decimallatitude, decimallongitude, 9) AS h9,
            h3_latlng_to_cell(decimallatitude, decimallongitude, 10) AS h10
        FROM read_parquet(?)
        WHERE decimallatitude IS NOT NULL 
          AND decimallongitude IS NOT NULL
          AND decimallatitude BETWEEN -90 AND 90
          AND decimallongitude BETWEEN -180 AND 180
    )
    SELECT DISTINCT h0 FROM data_with_h3
    """
    
    # First, get all unique h0 values in this chunk
    h0_values = con.execute(query, [source_file]).fetchall()
    
    print(f"Found {len(h0_values)} unique h0 partitions in this chunk")
    
    # Process each h0 partition separately
    for (h0_val,) in h0_values:
        h0_hex = format(h0_val, 'x')  # Convert to hex string
        output_path = f"{output_base}/h0={h0_hex}"
        
        print(f"  Writing h0={h0_hex} to {output_path}")
        
        # Write this h0 partition
        # Using CREATE OR REPLACE would overwrite, so we write to unique filenames
        import time
        timestamp = int(time.time() * 1000)
        
        write_query = f"""
        COPY (
            SELECT 
                *,
                h3_latlng_to_cell(decimallatitude, decimallongitude, 0) AS h0,
                h3_latlng_to_cell(decimallatitude, decimallongitude, 1) AS h1,
                h3_latlng_to_cell(decimallatitude, decimallongitude, 2) AS h2,
                h3_latlng_to_cell(decimallatitude, decimallongitude, 3) AS h3,
                h3_latlng_to_cell(decimallatitude, decimallongitude, 4) AS h4,
                h3_latlng_to_cell(decimallatitude, decimallongitude, 5) AS h5,
                h3_latlng_to_cell(decimallatitude, decimallongitude, 6) AS h6,
                h3_latlng_to_cell(decimallatitude, decimallongitude, 7) AS h7,
                h3_latlng_to_cell(decimallatitude, decimallongitude, 8) AS h8,
                h3_latlng_to_cell(decimallatitude, decimallongitude, 9) AS h9,
                h3_latlng_to_cell(decimallatitude, decimallongitude, 10) AS h10
            FROM read_parquet(?)
            WHERE decimallatitude IS NOT NULL 
              AND decimallongitude IS NOT NULL
              AND decimallatitude BETWEEN -90 AND 90
              AND decimallongitude BETWEEN -180 AND 180
              AND h3_latlng_to_cell(decimallatitude, decimallongitude, 0) = ?
        ) TO '{output_path}/chunk_{timestamp}.parquet'
        (FORMAT 'parquet', COMPRESSION 'snappy')
        """
        
        con.execute(write_query, [source_file, h0_val])
        print(f"    ✓ Completed h0={h0_hex}")

def main():
    """Main processing function."""
    
    # Configuration
    source_path = "s3://gbif-open-data-us-east-1/occurrence/2025-06-01/occurrence.parquet"
    output_base = "s3://public-gbif/2025-06/chunks"
    
    print("=" * 80)
    print("GBIF H3 Processing")
    print("=" * 80)
    print(f"Source: {source_path}")
    print(f"Output: {output_base}")
    print("=" * 80)
    
    # Initialize DuckDB
    print("\n[1/3] Initializing DuckDB with extensions...")
    con = setup_duckdb()
    print("  ✓ Extensions loaded: httpfs, h3")
    
    # Get list of files to process
    print("\n[2/3] Discovering source files...")
    
    # Process using glob pattern to handle all files
    source_glob = f"{source_path}/**/*.parquet"
    
    # Get file list
    try:
        file_query = f"""
        SELECT DISTINCT filename 
        FROM read_parquet('{source_glob}', filename=true)
        """
        files = con.execute(file_query).fetchall()
        print(f"  ✓ Found {len(files)} files to process")
    except Exception as e:
        print(f"  ! Could not list files, will process with glob pattern: {e}")
        files = [(source_glob,)]
    
    # Process each file
    print("\n[3/3] Processing files...")
    for idx, (file_path,) in enumerate(files, 1):
        print(f"\n--- File {idx}/{len(files)} ---")
        try:
            process_gbif_chunk(con, file_path, output_base)
            print(f"  ✓ File {idx} completed successfully")
        except Exception as e:
            print(f"  ✗ Error processing file {idx}: {e}")
            continue
    
    print("\n" + "=" * 80)
    print("Processing complete!")
    print("=" * 80)
    
    # Close connection
    con.close()

if __name__ == "__main__":
    main()
