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
    cleanup: bool = True,
) -> None:
    """
    Repartition chunks by h0 for efficient spatial querying.
    
    Args:
        chunks_dir: S3 URL or local path to chunks directory
        output_dir: S3 URL or local path to output directory
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
    
    print('Reading chunks and writing to local directory with h0 partitioning...')
    con.read_parquet(f'{chunks_dir}/*.parquet').to_parquet(f'{local_dir}/', partition_by='h0')
    
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
