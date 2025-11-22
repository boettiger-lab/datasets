import ibis
from ibis import _
from cng.utils import *
 
con = ibis.duckdb.connect()
set_secrets(con, "", "", "s3-west.nrp-nautilus.io")
con.raw_sql("SET preserve_insertion_order=false") # saves RAM


(con
    .read_parquet("s3://public-wetlands/nwi/bigchunks/**")
    .to_parquet("s3://public-wetlands/nwi/hex/", partition_by = "h0")
)

