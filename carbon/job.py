import argparse
import os
import sys
from osgeo import gdal
import ibis
from cng.utils import *  # noqa
from cng.h3 import *  # noqa
from ibis import _


def main():
  parser = argparse.ArgumentParser(description="Process vulnerable carbon tile for a given hex index i")
  parser.add_argument("--i", type=int, required=True, help="Hex index i to process (matches column i in h0-valid.parquet)")
  parser.add_argument("--zoom", type=int, default=8, help="H3 resolution to aggregate to (default 8)")
  parser.add_argument("--input-url", default="/vsis3/public-carbon/cogs/vulnerable_c_total_2018.tif", help="Input raster URL")
  args = parser.parse_args()

  i = args.i
  zoom = args.zoom

  gdal.DontUseExceptions()
  install_h3()
  con = ibis.duckdb.connect(extensions=["spatial", "h3"])
  print("Connected to DuckDB", flush=True)

  # internal endpoint on NRP does not use ssl.
  set_secrets(
    con,
    key=os.getenv("AWS_ACCESS_KEY_ID"),
    secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
    endpoint=os.getenv("AWS_S3_ENDPOINT"),
    use_ssl="FALSE",
  )
  print("AWS secrets configured", flush=True)

  print(f"Input URL: {args.input_url}", flush=True)

  import ibis.expr.datatypes as dt

  @ibis.udf.scalar.builtin
  def ST_GeomFromText(geom) -> dt.geometry:  # noqa: D401
    ...

  @ibis.udf.scalar.builtin
  def ST_MakeValid(geom) -> dt.geometry:  # noqa: D401
    ...

  df = con.read_parquet("s3://public-grids/hex/h0-valid.parquet")
  wkt = df.filter(_.i == i).geom.execute().set_crs("EPSG:4326").to_wkt()[0]
  h0 =  df.filter(_.i == i).h0.execute()[0]
  # Prefer https vsicurl to avoid needing AWS credentials for the public file
  input_url = args.input_url
  if input_url.startswith("/vsis3/"):
    # keep as is, else allow override
    pass

  gdal.Warp(
    "/tmp/carbon.xyz",
    input_url,
    dstSRS="EPSG:4326",
    cutlineWKT=wkt,
    cropToCutline=True,
  )

  # compute h0 id for output path (from the polygon we just used)
  (
    con.read_csv(
      "/tmp/carbon.xyz",
      delim=" ",
      columns={"X": "FLOAT", "Y": "FLOAT", "Z": "INTEGER"},
    )
    #.mutate(Z=ibis.ifelse(_.Z == 65535, None, _.Z))
    .filter(_.Z != 65535)
    .mutate(
      h0=h3_latlng_to_cell_string(_.Y, _.X, 0),  # base
      h_zoom=h3_latlng_to_cell_string(_.Y, _.X, zoom),
    )
    .select(_.Z, _.h_zoom, _.h0)
    .rename({f"h{zoom}": "h_zoom"})
    .rename(carbon = "Z")
    .to_parquet(
      f"s3://public-carbon/hex2/vulnerable-carbon/h0={h0}/data_0.parquet"
    )
  )
  print("Finished writing parquet", flush=True)


if __name__ == "__main__":
  main()

