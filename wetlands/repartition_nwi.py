
import ibis
from ibis import _
from cng.utils import *
from cng.h3 import * 
import os

os.makedirs("/tmp/hex", exist_ok=True)

con = ibis.duckdb.connect("/tmp/duck.db", extensions = ["spatial", "h3"])
con.raw_sql("INSTALL h3 FROM community; LOAD h3;")

set_secrets(con)

#con.raw_sql("SET threads TO 100")
con.raw_sql("SET preserve_insertion_order=false") # saves RAM

#con.raw_sql("create table wetlands as select * from read_parquet('s3://public-wetlands/nwi/chunks/**')")
#con.raw_sql("copy wetlands to 's3://public-wetlands/nwi/hex' (FORMAT PARQUET, PARTITION_BY (h1))")



@ibis.udf.scalar.builtin
def h3_cell_to_parent(cell, zoom: int) -> str:
    ...



chunks = con.read_parquet("s3://public-wetlands/nwi/chunks/**")
chunks_h1 = chunks.mutate(h1 = h3_cell_to_parent(_.h8, 1))
h1 = chunks_h1.select("h1").distinct().execute()["h1"].tolist()


print("writing partitions:")
for h in h1:
    print(h)
    chunks.filter(h3_cell_to_parent(_.h8, 1) == h).to_parquet(f"/tmp/hex/h1={h}/data_0.parquet")


con.open_parquet("/tmp/hex").to_parquet("s3://public-wetlands/nwi/hex/", partition_by = "h1")

