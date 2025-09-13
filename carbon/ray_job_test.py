import os
import pathlib
import ray
from osgeo import gdal
import ibis
from cng.utils import *
from cng.h3 import *
from ibis import _

@ray.remote
def process_hex_task(h0_hex, geom_wkt, input_url, aws_config, zoom=8):
    """Process a single h0 hex - runs on Ray worker"""
    
    # Setup worker-local connections
    gdal.DontUseExceptions()
    install_h3()
    
    # Use unique DB name per task to avoid conflicts
    worker_id = ray.get_runtime_context().get_worker_id()
    con = ibis.duckdb.connect(f"/tmp/duck_{worker_id}_{h0_hex}.db", 
                             extensions=["spatial", "h3"])
    
    # Configure AWS credentials on worker
    set_secrets(con, 
                key=aws_config["key"], 
                secret=aws_config["secret"], 
                endpoint=aws_config["endpoint"],
                use_ssl="FALSE")
    
    # Use unique temp file per task
    temp_file = f"/tmp/carbon_{worker_id}_{h0_hex}.xyz"
    
    import ibis.expr.datatypes as dt
    @ibis.udf.scalar.builtin
    def ST_GeomFromText(geom) -> dt.geometry:
        ...
    
    try:
        print(f"Worker {worker_id}: Processing h0={h0_hex}: cropping raster")
        gdal.Warp(temp_file, input_url, 
                 dstSRS='EPSG:4326', 
                 cutlineWKT=geom_wkt, 
                 cropToCutline=True)
        
        print(f"Worker {worker_id}: Processing h0={h0_hex}: computing zoom {zoom} hexes")
        (con
            .read_csv(temp_file, 
                     delim=' ', 
                     columns={'X': 'FLOAT', 'Y': 'FLOAT', 'Z': 'INTEGER'})
            .mutate(h0=h3_latlng_to_cell_string(_.Y, _.X, zoom),
                   h8=h3_latlng_to_cell_string(_.Y, _.X, zoom))
            .mutate(Z=ibis.ifelse(_.Z == 65535, None, _.Z)) 
            .to_parquet(f"s3://public-carbon/hex/vulnerable-carbon/h0={h0_hex}/vulnerable-total-carbon-2018-h{zoom}.parquet")
        )
        
        # Cleanup
        pathlib.Path(temp_file).unlink(missing_ok=True)
        con.disconnect()
        
        return f"Successfully processed h0={h0_hex}"
        
    except Exception as e:
        # Cleanup on error
        pathlib.Path(temp_file).unlink(missing_ok=True)
        if 'con' in locals():
            con.disconnect()
        return f"Error processing h0={h0_hex}: {e}"


def main():
    # Initialize Ray - this will connect to the cluster when run from head pod
    ray.init()
    
    # Setup main database connection
    gdal.DontUseExceptions()
    install_h3()
    con = ibis.duckdb.connect("/tmp/duck_main.db", extensions=["spatial", "h3"])
    
    # AWS configuration
    aws_config = {
        "key": os.getenv("AWS_ACCESS_KEY_ID"),
        "secret": os.getenv("AWS_SECRET_ACCESS_KEY"), 
        "endpoint": os.getenv("AWS_S3_ENDPOINT")
    }
    
    set_secrets(con, 
                key=aws_config["key"], 
                secret=aws_config["secret"], 
                endpoint=aws_config["endpoint"],
                use_ssl="FALSE")
    
    input_url = "/vsicurl/https://minio.carlboettiger.info/public-carbon/cogs/vulnerable_c_total_2018.tif"
    
    import ibis.expr.datatypes as dt
    @ibis.udf.scalar.builtin
    def ST_GeomFromText(geom) -> dt.geometry:
        ...
    
    # Load the h0 data
    print("Loading h0 hex data...")
    df = (con
          .read_parquet("s3://public-grids/hex/h0.parquet")
          .mutate(geom=ST_GeomFromText(_.geom))
          .mutate(h0=_.h0.lower())
          .execute()
          .set_crs("EPSG:4326")
    )
    
    # TESTING: Only process first 4 hexes
    print(f"Total hexes available: {df.shape[0]}")
    print("TESTING MODE: Processing only first 4 hexes")
    df = df.head(4)
    
    print(f"Starting parallel processing of {df.shape[0]} h0 hexes using Ray")
    print(f"Ray cluster resources: {ray.cluster_resources()}")
    
    # Submit all tasks to Ray cluster
    futures = []
    for i in range(df.shape[0]):
        future = process_hex_task.remote(
            h0_hex=df.h0[i],
            geom_wkt=df.geom[i], 
            input_url=input_url,
            aws_config=aws_config,
            zoom=8
        )
        futures.append(future)
        print(f"Submitted task {i+1}/{df.shape[0]}: h0={df.h0[i]}")
    
    print(f"All {len(futures)} tasks submitted. Waiting for completion...")
    
    # Wait for all tasks to complete and collect results
    results = ray.get(futures)
    
    # Print summary
    successful = len([r for r in results if "Successfully" in r])
    failed = len([r for r in results if "Error" in r])
    
    print(f"\n=== Processing Complete ===")
    print(f"Total hexes: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    
    # Print all results
    for result in results:
        print(f"  {result}")
    
    con.disconnect()
    ray.shutdown()


if __name__ == "__main__":
    main()
