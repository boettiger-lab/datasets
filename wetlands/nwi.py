import argparse

import ibis
from ibis import _
from cng.utils import *
from cng.h3 import * 
import os


@ibis.udf.scalar.builtin
def h3_polygon_wkt_to_cells_string (geom, int) -> str:
    ...


def geom_to_cell(df, zoom=8, keep_cols=None):
    con = df.get_backend()
    
    # Default to keeping all columns except geom if not specified
    if keep_cols is None:
        keep_cols = [col for col in df.columns if col != 'geom']
    
    # Build column list for SELECT statements
    col_list = ', '.join(keep_cols)
    
    # all types must be multi-polygons
    cases = ibis.cases(
        (df.geom.geometry_type() == 'POLYGON', ST_Multi(df.geom)),
        else_=df.geom,
    )
    
    df = df.mutate(geom=cases)
    sql = ibis.to_sql(df)
    
    expr = f'''
        WITH t1 AS (
            SELECT {col_list}, UNNEST(ST_Dump(ST_GeomFromWKB(geom))).geom AS geom 
            FROM ({sql})
        ) 
        SELECT *, h3_polygon_wkt_to_cells_string(geom, {zoom}) AS h3id FROM t1
    '''

    out = con.sql(expr)
    return out



def main():
    parser = argparse.ArgumentParser(description="Process polygon file i to zoom z")
    parser.add_argument("--i", type=int, default = 1, help="File index i to process")
    parser.add_argument("--zoom", type=int, default=8, help="H3 resolution to aggregate to (default 8)")
    parser.add_argument("--input-url", default = "s3://us-west-2.opendata.source.coop/giswqs/nwi/wetlands/**",  help="Input geoparquet")
    parser.add_argument("--output-url", default = "s3://public-wetlands/nwi/chunks",  help="Output geoparquet bucket")
    args = parser.parse_args()


    con = ibis.duckdb.connect(extensions = ["spatial", "h3"])
    install_h3()
    # Must used scoped secrets with different names for the different endpoints
    set_secrets(con, name = "minio") # read/write using AWS env var credentials
    set_secrets(con, "", "", endpoint = "s3.amazonaws.com", region="us-west-2", name = "source", bucket = "us-west-2.opendata.source.coop")


    SOURCE = args.input_url

    table =(con
        .read_parquet(SOURCE, filename = True)
        .select('geometry', 'ATTRIBUTE', 'WETLAND_TYPE', 'filename')
        .rename(geom = "geometry")
        .mutate(state_code=_.filename.re_extract(r"([A-Z]{2})_Wetlands.parquet", 1))
        .mutate(geom =  _.geom.convert('EPSG:5070','EPSG:4326'))
        .drop('filename')
    )

    # Read parquet file

    CHUNK_SIZE = 2048
    #MEMORY_LIMIT='20GB'
    #con.raw_sql(f"SET memory_limit='{MEMORY_LIMIT}';")
    OUTPUT_PATH="s3://public-wetlands/nwi/"

    # Get total row count and calculate chunks
    total_rows = table.count().execute()
    num_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE

    print(f"Total rows: {total_rows:,}")
    print(f"Chunk size: {CHUNK_SIZE:,}")
    print(f"Number of chunks: {num_chunks}")

    chunk_id = 0
    offset = chunk_id * CHUNK_SIZE
    print(f"\nProcessing chunk {chunk_id + 1}/{num_chunks} (rows {offset:,} to {min(offset + CHUNK_SIZE, total_rows):,})")

    chunk = table.limit(CHUNK_SIZE, offset=offset)
    result = geom_to_cell(chunk, zoom=8).mutate(h8 = _.h3id.unnest()).drop('h3id')

    output_file = f"{args.output_url}chunks/chunk_{chunk_id:06d}.parquet"
    result.to_parquet(output_file)

    print(f"  ✓ Chunk {chunk_id} written")


if __name__ == "__main__":
    main()
