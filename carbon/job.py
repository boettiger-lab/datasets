
import os
import pathlib
import sys
from osgeo import gdal
import ibis
from cng.utils import *
from cng.h3 import *
from ibis import _

gdal.DontUseExceptions()
install_h3()
con = ibis.duckdb.connect("/tmp/duck.db", extensions = ["spatial", "h3"])

con.raw_sql("SET memory_limit = '70GB';")
con.raw_sql('''
SET temp_directory = '/tmp/duckdb_swap';
SET max_temp_directory_size = '100GB';
            ''')

print("Connected to DuckDB", flush=True)

# internal endpoint on NRP does not use ssl. 
set_secrets(con, 
            key = os.getenv("AWS_ACCESS_KEY_ID"), 
            secret = os.getenv("AWS_SECRET_ACCESS_KEY"), 
            endpoint = os.getenv("AWS_S3_ENDPOINT"),
            use_ssl = "FALSE")
print("AWS secrets configured", flush=True)

input_url = "/vsis3/public-carbon/cogs/vulnerable_c_total_2018.tif"
print(f"Input URL: {input_url}", flush=True)

import ibis.expr.datatypes as dt
@ibis.udf.scalar.builtin
def ST_GeomFromText(geom) -> dt.geometry:
    ...
@ibis.udf.scalar.builtin
def ST_MakeValid(geom) -> dt.geometry:
    ...

print("Loading h0 parquet file...", flush=True)
df = (con
      .read_parquet("s3://public-grids/hex/h0.parquet")
      .mutate(geom = ST_MakeValid(ST_GeomFromText(_.geom)))
      .mutate(h0 = _.h0.lower())
      .execute()
      .set_crs("EPSG:4326")
)
print(f"Loaded h0 data with {df.shape[0]} rows", flush=True)
con.disconnect()

zoom = 8
for i in range(df.shape[0]):

    # reset connection each time
    con = ibis.duckdb.connect("/tmp/duck.db", extensions = ["spatial", "h3"])
    con.raw_sql("SET memory_limit = '70GB';")
    con.raw_sql('''
    SET temp_directory = '/tmp/duckdb_swap';
    SET max_temp_directory_size = '100GB';
                ''')
    set_secrets(con, 
            key = os.getenv("AWS_ACCESS_KEY_ID"), 
            secret = os.getenv("AWS_SECRET_ACCESS_KEY"), 
            endpoint = os.getenv("AWS_S3_ENDPOINT"),
            use_ssl = "FALSE")



    wkt = df.geom[i]
    h0 = df.h0[i]
    zoom = 8
    print(f"i={i}: cropping raster to h0={h0}\n")
    try:
        gdal.Warp("/tmp/carbon.xyz", input_url, dstSRS = 'EPSG:4326', cutlineWKT = wkt, cropToCutline = True)
        print(f"i={i}: computing zoom {zoom} hexes:\n")
        (con
            .read_csv("/tmp/carbon.xyz", 
                    delim = ' ', 
                    columns = {'X': 'FLOAT', 'Y': 'FLOAT', 'Z': 'INTEGER'})
            .mutate(h0 = h3_latlng_to_cell_string(_.Y, _.X, zoom),
                    h8 = h3_latlng_to_cell_string(_.Y, _.X, zoom))
            .mutate(Z = ibis.ifelse(_.Z == 65535, None, _.Z)) 
            .to_parquet(f"s3://public-carbon/hex/vulnerable-carbon/h0={h0}/vulnerable-total-carbon-2018-h{zoom}.parquet")
        )
        con.disconnect()
    except Exception as e:
        print(f"Error processing item {i}: {e}")




