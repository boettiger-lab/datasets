
import ibis
from ibis import _
from cng.utils import *
from cng.h3 import * 
con = ibis.duckdb.connect("/tmp/duck.db", extensions = ["spatial", "h3"])
install_h3()

set_secrets(con)

#con.raw_sql("SET threads TO 100")
con.raw_sql("SET preserve_insertion_order=false") # saves RAM

print("Re-paritioning....")
(con
 .read_parquet("s3://public-wetlands/nwi/chunks/**")
 .mutate(h1 = h3_cell_to_parent(_.h8, 1))
 .to_parquet("s3://public-wetlands/nwi/hex/", partition_by="h1")
)
print("Done!")

