
import os
import pathlib
from osgeo import gdal
import ibis
from ibis import _
from cng.utils import *
from cng.h3 import *

gdal.DontUseExceptions()
con = ibis.duckdb.connect("/tmp/duck.db",extensions = ["spatial", "h3"])
install_h3()
set_secrets(con)

input_url = "/vsicurl/https://minio.carlboettiger.info/public-carbon/cogs/vulnerable_c_total_2018.tif"
df = con.read_parquet("s3://public-grids/hex/h0.parquet").mutate(h0 = _.h0.lower()).execute()

i = 100
wkt = df.geom[i]
h0 = df.h0[i]

gdal.Warp("/tmp/carbon.xyz", input_url, dstSRS = 'EPSG:4326', cutlineWKT = wkt, cropToCutline = True)
(con
  .read_csv("/tmp/carbon.xyz", 
            delim = ' ', 
            columns = {'X': 'FLOAT', 'Y': 'FLOAT', 'Z': 'INTEGER'})
  .mutate(h0 = h3_latlng_to_cell_string(_.Y, _.X, zoom),
          h8 = h3_latlng_to_cell_string(_.Y, _.X, zoom))
  .mutate(Z = ibis.ifelse(_.Z == 65535, None, _.Z)) 
 .to_parquet("/tmp/test.parquet")
  #.to_parquet(f"s3://public-carbon/hex/vulnerable-carbon/h0={h0}/vulnerable-total-carbon-2018-h{zoom}.parquet")
)
pathlib.Path("/tmp/carbon.xyz").unlink()
con.read_parquet("/tmp/test.parquet")

