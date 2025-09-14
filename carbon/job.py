
import os
import pathlib
from osgeo import gdal
import ibis
from cng.utils import *
from cng.h3 import *
from ibis import _

gdal.DontUseExceptions()
install_h3()
con = ibis.duckdb.connect(extensions = ["spatial", "h3"])

# internal endpoint on NRP does not use ssl. 
set_secrets(con, 
            key = os.getenv("AWS_ACCESS_KEY_ID"), 
            secret = os.getenv("AWS_SECRET_ACCESS_KEY"), 
            endpoint = os.getenv("AWS_S3_ENDPOINT"),
            use_ssl = "FALSE")

input_url = "/vsis3/public-carbon/cogs/vulnerable_c_total_2018.tif"

import ibis.expr.datatypes as dt
@ibis.udf.scalar.builtin
def ST_GeomFromText(geom) -> dt.geometry:
    ...

df = (con
      .read_parquet("s3://public-grids/hex/h0.parquet")
      .mutate(geom = ST_GeomFromText(_.geom))
      .mutate(h0 = _.h0.lower())
      .execute()
      .set_crs("EPSG:4326")
)

zoom = 8
for i in range(df.shape[0]):
    wkt = df.geom[i]
    h0 = df.h0[i]
    zoom = 8
    print(f"i={i}: cropping raster to h0={h0}\n")
    try:
        gdal.Warp("/vsis3/public-carbon/carbon.xyz", input_url, dstSRS = 'EPSG:4326', cutlineWKT = wkt, cropToCutline = True)
        print(f"i={i}: computing zoom {zoom} hexes:\n")
        (con
            .read_csv("s3://public-carbon/carbon.xyz", 
                    delim = ' ', 
                    columns = {'X': 'FLOAT', 'Y': 'FLOAT', 'Z': 'INTEGER'})
            .mutate(h0 = h3_latlng_to_cell_string(_.Y, _.X, zoom),
                    h8 = h3_latlng_to_cell_string(_.Y, _.X, zoom))
            .mutate(Z = ibis.ifelse(_.Z == 65535, None, _.Z)) 
            .to_parquet(f"s3://public-carbon/hex/vulnerable-carbon/h0={h0}/vulnerable-total-carbon-2018-h{zoom}.parquet")
        )

    except Exception as e:
        print(f"Error processing item {i}: {e}")




