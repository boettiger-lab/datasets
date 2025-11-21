
import ibis
from ibis import _
from cng.utils import *
from cng.h3 import * 
import os
import boto3
import math
import concurrent.futures
import shutil
from botocore.config import Config

os.makedirs("/tmp/hex", exist_ok=True)

# Configure S3 client for listing files
endpoint = os.getenv("AWS_S3_ENDPOINT")
if endpoint and not endpoint.startswith("http"):
    endpoint = f"http://{endpoint}"

s3 = boto3.client('s3', 
    endpoint_url=endpoint,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    config=Config(max_pool_connections=50)
)

BUCKET = "public-wetlands"
PREFIX = "nwi/chunks/"
OUTPUT_BASE = "s3://public-wetlands/nwi/hex"

print("Listing files...")
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=BUCKET, Prefix=PREFIX)
files = []
for page in pages:
    if 'Contents' in page:
        for obj in page['Contents']:
            key = obj['Key']
            if key.endswith('.parquet'):
                files.append(f"s3://{BUCKET}/{key}")

print(f"Found {len(files)} files.")

# Process in batches to avoid S3 timeouts and memory issues
BATCH_SIZE = 100
num_batches = math.ceil(len(files) / BATCH_SIZE)

# Check which batches are already complete by checking for output files
print("Checking for completed batches...")
completed_batches = set()
try:
    # List existing output files to determine which batches completed
    output_paginator = s3.get_paginator('list_objects_v2')
    output_pages = output_paginator.paginate(Bucket=BUCKET, Prefix="nwi/hex/")
    for page in output_pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                # Extract batch number from part_{i}.parquet
                if 'part_' in key:
                    try:
                        batch_num = int(key.split('part_')[1].split('.')[0])
                        completed_batches.add(batch_num)
                    except (IndexError, ValueError):
                        pass
    print(f"Found {len(completed_batches)} completed batches: {sorted(completed_batches)[:10]}{'...' if len(completed_batches) > 10 else ''}")
except Exception as e:
    print(f"Warning: Could not check completed batches: {e}")

# Use workspace for temp files if available (k8s volume), else /tmp
TEMP_BASE = "/workspace/tmp" if os.path.exists("/workspace") else "/tmp"
os.makedirs(TEMP_BASE, exist_ok=True)

for i in range(num_batches):
    # Skip already completed batches
    if i in completed_batches:
        print(f"Skipping batch {i+1}/{num_batches} (already completed)")
        continue
        
    batch_files = files[i*BATCH_SIZE : (i+1)*BATCH_SIZE]
    print(f"Processing batch {i+1}/{num_batches} ({len(batch_files)} files)...")
    
    local_dir = f"{TEMP_BASE}/batch_{i}"
    os.makedirs(local_dir, exist_ok=True)
    
    # Initialize DuckDB for this batch to ensure clean state and no temp file leakage
    db_path = f"{TEMP_BASE}/duck_{i}.db"
    con = ibis.duckdb.connect(db_path, extensions = ["spatial", "h3"])
    con.raw_sql("INSTALL h3 FROM community; LOAD h3;")
    con.raw_sql(f"SET temp_directory='{TEMP_BASE}/duck_temp_{i}.tmp'")
    set_secrets(con)
    con.raw_sql("SET preserve_insertion_order=false")
    
    try:
        # Download files in parallel
        print("  Downloading files...")
        def download_one(s3_path):
            key = s3_path.replace(f"s3://{BUCKET}/", "")
            fname = os.path.basename(key)
            local_path = os.path.join(local_dir, fname)
            s3.download_file(BUCKET, key, local_path)
            return local_path

        # Reduce parallel downloads to limit memory spikes
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            local_files = list(executor.map(download_one, batch_files))

        print(f"  Downloaded {len(local_files)} files, total size: {sum(os.path.getsize(f) for f in local_files) / (1024**3):.2f} GB")
        
        # Construct file list for read_parquet
        file_list_str = ", ".join([f"'{f}'" for f in local_files])
        
        print("  Computing distinct h1 values from files...")
        # First pass: just get the distinct h1 values without loading all data
        h1_result = con.raw_sql(f"""
            SELECT DISTINCT h3_cell_to_parent(h8, 1) as h1 
            FROM read_parquet([{file_list_str}])
        """).fetchall()
        h1_values = [row[0] for row in h1_result]
        
        local_out_dir = f"{TEMP_BASE}/batch_{i}_out"
        os.makedirs(local_out_dir, exist_ok=True)

        print(f"  Writing {len(h1_values)} partitions locally (streaming from source files)...")
        local_files_to_upload = []

        for j, h in enumerate(h1_values):
            if j % 10 == 0:
                print(f"    Processing partition {j+1}/{len(h1_values)}...")
            # Local path
            local_partition_dir = os.path.join(local_out_dir, f"h1={h}")
            os.makedirs(local_partition_dir, exist_ok=True)
            local_file_path = os.path.join(local_partition_dir, f"part_{i}.parquet")
            
            # S3 Key (OUTPUT_BASE is s3://public-wetlands/nwi/hex)
            # We want nwi/hex/h1={h}/part_{i}.parquet
            s3_key = f"nwi/hex/h1={h}/part_{i}.parquet"
            
            # Stream directly from source files to output - don't materialize in memory
            con.raw_sql(f"""
                COPY (
                    SELECT * EXCLUDE(h1) 
                    FROM (
                        SELECT *, h3_cell_to_parent(h8, 1) as h1 
                        FROM read_parquet([{file_list_str}])
                    ) 
                    WHERE h1 = '{h}'
                ) 
                TO '{local_file_path}' (FORMAT PARQUET)
            """)
            
            local_files_to_upload.append((local_file_path, s3_key))

        print(f"  Uploading {len(local_files_to_upload)} partitions to S3...")
        
        def upload_one(args):
            local_path, key = args
            try:
                s3.upload_file(local_path, BUCKET, key)
                return True
            except Exception as e:
                print(f"Failed to upload {key}: {e}")
                return False

        # Reduce parallel uploads to limit memory spikes
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(upload_one, local_files_to_upload))
        
        print(f"  Uploads complete.")
            
        failed_count = results.count(False)
        if failed_count > 0:
             raise Exception(f"{failed_count} uploads failed")
            
        print(f"  Batch {i+1} completed.")
        
        # Clean up - no batch_temp table to drop anymore
        con.disconnect() # Close connection
        if os.path.exists(db_path):
            os.remove(db_path)
        if os.path.exists(f"{TEMP_BASE}/duck_temp_{i}.tmp"):
            shutil.rmtree(f"{TEMP_BASE}/duck_temp_{i}.tmp")
            
        if os.path.exists(local_out_dir):
            shutil.rmtree(local_out_dir)
        
    except Exception as e:
        print(f"  Error in batch {i+1}: {e}")
        try:
            con.disconnect()
        except:
            pass
        
    finally:
        if os.path.exists(local_dir):
            shutil.rmtree(local_dir)

print("Done!")

