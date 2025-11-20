
import ibis
from ibis import _
from cng.utils import *
from cng.h3 import * 
con = ibis.duckdb.connect("/tmp/duck.db", extensions = ["spatial", "h3"])
con.raw_sql("INSTALL h3 FROM community; LOAD h3;")

set_secrets(con)

#con.raw_sql("SET threads TO 100")
con.raw_sql("SET preserve_insertion_order=false") # saves RAM

con.raw_sql("create table wetlands from scan_parquet('s3://public-wetlands/nwi/chunks/**')")
con.raw_sql("copy wetlands to s3://public-wetlands/nwi/nwi_h8.parquet")