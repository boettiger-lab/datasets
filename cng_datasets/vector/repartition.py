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
from cng_datasets.vector.h3_tiling import identify_id_column


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
    
    # Read chunks (sparse format: ID_column, h10, h9, h8, h0)
    chunks = con.read_parquet(f'{chunks_dir}/*.parquet')
    chunk_cols = chunks.columns
    
    # Identify ID column in chunks
    # Prioritize _cng_fid (standard from convert_to_parquet), then _fid (fallback), then other non-h3 columns
    chunk_id_col = None
    priority_ids = ['_cng_fid', '_fid']
    
    # First check for priority IDs
    for priority_id in priority_ids:
        if priority_id in chunk_cols:
            chunk_id_col = priority_id
            break
    
    # If no priority ID found, look for any non-h3 column
    if not chunk_id_col:
        for col in chunk_cols:
            if not col.startswith('h') or not col[1:].isdigit():
                chunk_id_col = col
                break
    
    if not chunk_id_col:
        raise ValueError(f"Could not identify ID column in chunks. Columns: {chunk_cols}")
    
    print(f"Chunks ID column: {chunk_id_col}")
    
    # If source parquet provided, join back all attributes (except geometry)
    if source_parquet:
        print(f'Joining attributes from {source_parquet}...')
        
        # Read source to get column info
        source = con.read_parquet(source_parquet)
        source_cols = source.columns
        
        # Use helper to find geometry column (case-insensitive)
        col_lower_map = {col.lower(): col for col in source_cols}
        geom_col = None
        for name in ['geometry', 'geom', 'shape']:
            if name in col_lower_map:
                geom_col = col_lower_map[name]
                break
        
        # Verify the chunk ID column exists in source
        if chunk_id_col not in source_cols:
            raise ValueError(f"Chunk ID column '{chunk_id_col}' not found in source parquet. Source columns: {source_cols}")
        
        # Select all columns except geometry (keep original ID column name)
        attr_cols = [c for c in source_cols if c != geom_col]
        
        print(f'  Joining on column: {chunk_id_col}')
        print(f'  Adding {len(attr_cols) - 1} attribute columns')
        
        # Join chunks with attributes (using the ID column from chunks)
        result = chunks.inner_join(source.select(attr_cols), chunk_id_col)
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
