import ibis
from ibis import _
from cng.utils import *
 
con = ibis.duckdb.connect()
set_secrets(con)
con.raw_sql("SET preserve_insertion_order=false") # saves RAM
con.raw_sql("SET http_timeout=1200")
con.raw_sql("SET http_retries=30")
con.raw_sql("SET s3_socket_timeout=1200")


# remove NA
(con
    .read_parquet("s3://public-wetlands/nwi/bigchunks/**").
    .to_parquet("s3://public-wetlands/nwi/hex/", partition_by = "h0")
)

