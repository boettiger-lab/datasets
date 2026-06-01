# Test fixtures

## `ghs_pop_clip.tif`

Clipped from the GHS-POP 2020 mosaic at:
`https://s3-west.nrp-nautilus.io/public-high-seas/raw/ghs-pop-2020-cog.tif`

Region: bbox `3.0, 6.3, 3.6, 6.9` (Lagos, Nigeria) — chosen for high
population density (so many h9 cells contain nonzero values) within a
small (~500 KB) bounding box suitable for committing to git.

Regenerate with:

```
gdal_translate \
  -projwin 3.0 6.9 3.6 6.3 \
  -co COMPRESS=DEFLATE -co TILED=YES \
  -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 \
  /vsicurl/https://s3-west.nrp-nautilus.io/public-high-seas/raw/ghs-pop-2020-cog.tif \
  tests/fixtures/ghs_pop_clip.tif
```

Used by `tests/test_raster.py::test_h3_aggregation_conserves_mass` to verify
issue #84 (raster pipeline emits one row per warped pixel, not per H3 cell).
