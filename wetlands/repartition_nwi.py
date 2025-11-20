
import ibis
from ibis import _
from cng.utils import *
from cng.h3 import * 
import os
import boto3
import math

os.makedirs("/tmp/hex", exist_ok=True)

con = ibis.duckdb.connect("/tmp/duck.db", extensions = ["spatial", "h3"])
con.raw_sql("INSTALL h3 FROM community; LOAD h3;")

set_secrets(con)
con.raw_sql("SET preserve_insertion_order=false") # saves RAM

# Configure S3 client for listing files
s3 = boto3.client('s3', 
    endpoint_url=os.getenv("AWS_S3_ENDPOINT"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
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
BATCH_SIZE = 500
num_batches = math.ceil(len(files) / BATCH_SIZE)

for i in range(num_batches):
    batch_files = files[i*BATCH_SIZE : (i+1)*BATCH_SIZE]
    print(f"Processing batch {i+1}/{num_batches} ({len(batch_files)} files)...")
    
    try:
        # Construct SQL list string for read_parquet
        file_list_str = ", ".join([f"'{f}'" for f in batch_files])
        
        # Load batch into a temp table, computing h1
        # Materializing to a temp table isolates the S3 read phase
        con.raw_sql(f"""
            CREATE OR REPLACE TABLE batch_temp AS 
            SELECT *, h3_cell_to_parent(h8, 1) as h1 
            FROM read_parquet([{file_list_str}])
        """)
        
        # Get distinct h1s in this batch
        h1_values = con.table("batch_temp").select("h1").distinct().execute()["h1"].tolist()
        
        print(f"  Writing {len(h1_values)} partitions...")
        for h in h1_values:
            # Write partition file for this batch
            # Using a unique filename (part_{i}.parquet) prevents overwrites between batches
            target_path = f"{OUTPUT_BASE}/h1={h}/part_{i}.parquet"
            
            con.raw_sql(f"""
                COPY (SELECT * EXCLUDE(h1) FROM batch_temp WHERE h1 = '{h}') 
                TO '{target_path}' (FORMAT PARQUET)
            """)
            
        print(f"  Batch {i+1} completed.")
        
        # Clean up
        con.raw_sql("DROP TABLE batch_temp")
        
    except Exception as e:
        print(f"  Error in batch {i+1}: {e}")

print("Done!")

