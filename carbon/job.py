import ibis
from ibis import _
import os
from osgeo import gdal
from cng.utils import *
from cng.h3 import *
import pathlib

con = ibis.duckdb.connect("duck.db",extensions = ["spatial", "h3"])
install_h3()
set_secrets(con)

df = con.read_parquet("s3://public-grids/hex/h0.parquet").mutate(h0 = _.h0.lower()).execute()

#see `gdal.WarpOptions?` for details. Also see resampling options in warper for large data
# NOTE! dest given before input!
for i in range(df.shape[0]):
    wkt = df.geom[i]
    h0 = df.h0[i]
    zoom = 8
    print(h0)

    try:
        gdal.Warp("tmp-carbon.xyz", input_url, dstSRS = 'EPSG:4326', cutlineWKT = wkt, cropToCutline = True)
    
        (con
          .read_csv("tmp-carbon.xyz", 
                    delim = ' ', 
                    columns = {'X': 'FLOAT', 'Y': 'FLOAT', 'Z': 'INTEGER'})
          .mutate(h0 = h3_latlng_to_cell_string(_.Y, _.X, zoom),
                  h8 = h3_latlng_to_cell_string(_.Y, _.X, zoom))
          .mutate(Z = ibis.ifelse(_.Z == 65535, None, _.Z)) 
          .to_parquet(f"s3://public-carbon/hex/vulnerable-carbon/h0={h0}/vulnerable-total-carbon-2018-h{zoom}.parquet")
        )
        pathlib.Path("tmp-carbon.xyz").unlink()
    except Exception as e:
        print(f"Error processing item {i}: {e}")
    
#gdal.Warp(dest, input_url, dstSRS = 'EPSG:4326')

