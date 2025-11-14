import argparse

import ibis
from ibis import _
from cng.utils import *
from cng.h3 import * 
import os
import minio

# FIXME make this a generic in cng.h3 that selects all columns
def geom_to_cell (df, zoom = 8):
    con = df.get_backend() # ibis >= 10.0

    # First make sure we are using multipolygons everywhere and not a mix
    cases = ibis.cases(
        (df.geom.geometry_type() == 'POLYGON' , ST_Multi(df.geom)),
        else_=df.geom,
    )
    
    df = df.mutate(geom = cases)
    sql = ibis.to_sql(df)
    expr = f'''
        WITH t1 AS (
        SELECT ATTRIBUTE, WETLAND_TYPE, UNNEST(ST_Dump(ST_GeomFromWKB(geom))).geom AS geom 
        FROM ({sql})
        ) 
        SELECT *, h3_polygon_wkt_to_cells_string(geom, {zoom}) AS h3id  FROM t1
    '''

    out = con.sql(expr)
    return out



def main():
    parser = argparse.ArgumentParser(description="Process polygon file i to zoom z")
    parser.add_argument("--i", type=int, required=True, default = 1, help="File index i to process")
    parser.add_argument("--zoom", type=int, default=8, help="H3 resolution to aggregate to (default 8)")
    parser.add_argument("--input-url", default = "s3://us-west-2.opendata.source.coop/giswqs/nwi/wetlands",  help="Input geoparquet")
    parser.add_argument("--output-url", default = "s3://public-wetlands/nwi/hex",  help="Output geoparquet bucket")
    args = parser.parse_args()

    print(f"{args.output_url}")

    con = ibis.duckdb.connect("local.db", extensions = ["spatial", "h3"])
    install_h3()
    # Must used scoped secrets with different names for the different endpoints
    set_secrets(con, name = "minio", bucket = "public-wetlands") # read/write using AWS env var credentials
    set_secrets(con, "", "", endpoint = "s3.amazonaws.com", region="us-west-2", name = "source", bucket = "us-west-2.opendata.source.coop")


    mc = minio.Minio("s3.amazonaws.com", "", "", region="us-west-2")
    obj = mc.list_objects("us-west-2.opendata.source.coop", prefix = "giswqs/nwi/wetlands", recursive = True)
    obj_list = list((o.object_name.split('/')[-1] for o in obj))

    file = obj_list[i]
    print(file)


    nwi = con.read_parquet(f"{args.input_url}/{file}")
    state = file.split('_')[0]
    print(state)

    (
    geom_to_cell(nwi.rename(geom = "geometry"), zoom=8)
    .mutate(h8 = _.h3id.unnest())
    .to_parquet(f"{args.output_url}/state={state}.parquet")
    )



if __name__ == "__main__":
    main()
