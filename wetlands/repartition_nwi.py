
import ibis
from ibis import _
from cng.utils import *
con = ibis.duckdb.connect()
set_secrets(con)

print("Re-paritioning....")
(con
 .read_parquet("s3://public-wetlands/nwi/hexchunks/**")
 .to_parquet("s3://public-wetlands/nwi/hex/", partition_by="h0")
)
print("Done!")

