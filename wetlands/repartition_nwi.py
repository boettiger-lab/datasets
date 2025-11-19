
import ibis
from ibis import _
from cng.utils import *
con = ibis.duckdb.connect("/tmp/duck.db")
set_secrets(con)

#con.raw_sql("SET threads TO 100")

print("Re-paritioning....")
(con
 .read_parquet("s3://public-wetlands/nwi/chunks/**")
 .to_parquet("s3://public-wetlands/nwi/hex/", partition_by="h0")
)
print("Done!")

