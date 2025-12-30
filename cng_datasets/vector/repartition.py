"""
Repartition vector datasets by H3 resolution 0 cells.

Consolidates chunked processing outputs into h0-partitioned format
for efficient spatial querying.
"""

import os
import shutil
import subprocess
import ibis
from cng_datasets.storage.s3 import configure_s3_credentials


def repartition_by_h0(
    chunks_dir: str,
    output_dir: str,
    source_parquet: str = None,
    cleanup: bool = True,
) -> None:
    """
    Repartition chunks by h0 for efficient spatial querying.
    Joins back attribute columns from source parquet (without geometry).
    
    Args:
        chunks_dir: S3 URL or local path to chunks directory
        output_dir: S3 URL or local path to output directory
        source_parquet: S3 URL to original parquet (for joining attributes)
        cleanup: Whether to remove chunks directory after repartitioning
    """
    print(f"Repartitioning chunks from {chunks_dir} to {output_dir}")
    
    # Set up DuckDB connection with S3 credentials
    con = ibis.duckdb.connect()
    configure_s3_credentials(con)
    con.raw_sql('SET preserve_insertion_order=false')  # saves RAM
    con.raw_sql('SET http_timeout=1200')
    con.raw_sql('SET http_retries=30')
    
    # Create local temporary directory
    local_dir = '/tmp/hex'
    os.makedirs(local_dir, exist_ok=True)
    
    # Read chunks (contains: fid, h10, h9, h8, h0)
    chunks = con.read_parquet(f'{chunks_dir}/*.parquet')
    
    # If source parquet provided, join back all attributes (except geometry)
    if source_parquet:
        print(f'Joining attributes from {source_parquet}...')
        
        # Read source and identify geometry/ID column
        source = con.read_parquet(source_parquet)
        source_cols = source.columns
        
        # Find geometry column to exclude
        geom_col = None
        for col in ['geometry', 'geom', 'shape', 'GEOMETRY', 'GEOM', 'SHAPE']:
            if col in source_cols:
                geom_col = col
                break
        
        # Find ID column
        id_col = None
        for col in ['FID', 'fid', 'OBJECTID', 'objectid', 'ID', 'id', '_fid']:
            if col in source_cols:
                id_col = col
                break
        
        if not id_col:
            print('⚠ No ID column found in source, proceeding without attribute join')
            result = chunks
        else:
            # Select all columns except geometry, rename ID column to fid for join
            attr_cols = [c for c in source_cols if c != geom_col]
            if id_col != 'fid':
                # Rename ID column to match chunks
                source = source.rename({id_col: 'fid'})
                attr_cols = ['fid' if c == id_col else c for c in attr_cols]
            
            print(f'  Joining on column: {id_col} → fid')
            print(f'  Adding {len(attr_cols) - 1} attribute columns')
            
            # Join chunks with attributes
            result = chunks.inner_join(source.select(attr_cols), 'fid')
    else:
        print('No source parquet provided, proceeding without attribute join')
        result = chunks
    
    print('Writing to local directory with h0 partitioning...')
    result.to_parquet(f'{local_dir}/', partition_by='h0')
    
    print('Uploading partitioned data to S3...')
    con.read_parquet(f'{local_dir}/**/*.parquet').to_parquet(f'{output_dir}/', partition_by='h0')
    
    print('Cleaning up local directory...')
    shutil.rmtree(local_dir)
    
    print('✓ Repartitioning complete!')
    
    # Clean up chunks directory if requested
    if cleanup and chunks_dir.startswith('s3://'):
        print('Removing chunks directory from S3...')
        # Extract bucket and path from s3://bucket/path
        parts = chunks_dir.replace('s3://', '').split('/', 1)
        if len(parts) == 2:
            bucket, path = parts
            rclone_path = f'nrp:{bucket}/{path}'
        else:
            rclone_path = f'nrp:{parts[0]}'
        
        try:
            result = subprocess.run(
                ['rclone', 'purge', rclone_path],
                capture_output=True,
                text=True,
                timeout=300
            )
            if result.returncode == 0:
                print('✓ Chunks directory removed successfully')
            else:
                print(f'⚠ rclone cleanup warning: {result.stderr}')
        except Exception as e:
            print(f'⚠ Error during cleanup: {e}')
    
    print('✓ All done!')
