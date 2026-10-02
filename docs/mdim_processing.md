# Multidimensional (cube) processing

`cng-datasets mdim` hexes a **(time, lat, lon) cube** into H3 partitions, with
no per-slice COGs. The source can be zarr, netCDF, or anything else GDAL's
multidimensional API opens, and a series split across files (one netCDF per
year) can be passed as several `--input`s in time order. Non-spatial axes stay
as columns; they are never fanned out into jobs.

```bash
cng-datasets mdim \
  --input 'ZARR:"/vsicurl/https://cadcat.s3.us-west-2.amazonaws.com/loca2/ucsd/gfdl-esm4/ssp370/r1i1p1f1/day/tasmax/d03"' \
  --variable tasmax \
  --output-parquet s3://bucket/loca2/hex/ \
  --resolution 6 --parent-resolutions 0 \
  --h0-index 50 \
  --time-agg year --time-start 2040-01-01 --time-end 2069-12-31
```

Output uses the same layout as `raster`: `hex/h0={cell}/data_0.parquet`, or
`part-{cell}.parquet` at `--chunk-resolution` > 0, with completion markers for
`merge-chunks`. Each file has columns `h<res>`, the parents, the time key, and
one column per `--variable`.

## How it reads

GDAL reads **chunk-aligned slabs**, so a query transfers only the chunks it
overlaps. On the LOCA2 store, a 1-day, 1° read moved one compressed chunk
(72 MiB). A NEX-GDDP netCDF file reads in place over `/vsicurl/` (1.1 MiB for
the same window). DuckDB does everything after the read: H3 indexing, the
reduction and the parquet write. The DuckDB `zarr` extension was evaluated
first and scans whole stores in v0.1.1 (#181); it can come in later as a
second reader.

## Placement

How pixels are put into cells depends on their size relative to the cells:

| `--placement` | when (`auto`) | what |
|---|---|---|
| `aggregate` | pixels smaller than cells | each pixel centre goes to its H3 cell, and the cell reduces its pixels |
| `sample` | pixels larger than cells | each cell reads the pixel containing its centre |

Neither placement is area-weighted, so `--hex-resampling` is `mean`, `min` or
`max`, and **`sum` is refused**. Extensive or density variables should still go
through a 2-D COG and `cng-datasets raster` (`exact-extract`).

## Time

CF time is decoded from the coordinate's units and `calendar`: `standard`,
`gregorian`, `proleptic_gregorian`, `noleap`/`365_day`, `all_leap`/`366_day`
and `360_day`.

- `--time-agg none` writes a `time` DATE column. A calendar with dates that
  don't exist in the Gregorian calendar (`360_day`'s 30 February, `all_leap`'s
  29 February every year) is refused here.
- `month` writes `year` and `month` columns, and `year` writes `year`. Both are
  exact on every calendar.
- `--time-start` / `--time-end` (`YYYY-MM-DD`) restrict the range read.

## Limits (v1)

- Variables must be exactly (time, lat, lon) on a 1-D lat/lon grid. Depth,
  ensemble and other axes aren't supported yet.
- Each call processes one chunk; the generated fan-out (`mdim-workflow`) is
  next (#181).
