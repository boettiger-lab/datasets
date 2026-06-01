# Raster → H3 area-weighted aggregation (issue #84)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the warped-XYZ → centroid-assign Stage 1+2 path with per-hex area-weighted aggregation via `exactextract`, producing one parquet row per native H3 cell with mass-conserving values.

**Architecture:** For each h0 partition, polyfill the h0 polygon to enumerate native-resolution H3 cells inside it, materialize each cell's boundary polygon, and call `exactextract` once per chunk with the source raster + cell polygons. Output: one row per cell with the area-weighted reducer (`sum`/`mean`/`mode`) and parent cell columns via `h3_cell_to_parent`. The `gdal.Warp` → XYZ → DuckDB read path and the `_h3_res_to_degrees` helper are deleted.

**Tech Stack:** `exactextract` (PyPI, links against system GDAL/GEOS in container), DuckDB h3 community extension (`h3_polygon_wkt_to_cells`, `h3_cell_to_boundary_wkt`, `h3_cell_to_parent`), `rasterio` (for handing rasters to `exact_extract` with explicit nodata), `geopandas` (already a dep, for vector input to `exact_extract`).

**Issue:** https://github.com/boettiger-lab/datasets/issues/84

**Design decisions (locked in via brainstorm):**
- Clean break on `--hex-resampling` enum — only `sum`/`mean`/`mode` accepted.
- Wholesale replace the warped-XYZ path — no flag, no fallback.
- Verification target: real-data mass-conservation test against a clipped `ghs-pop-2020` tile.

---

## File Structure

**Modify:**
- `cng_datasets/raster/cog.py` — rewrite `process_h0_region` body; add `_hex_aggregate_h0`; remove `_h3_res_to_degrees`; tighten `hex_resampling` validation in `__init__`.
- `cng_datasets/cli.py` — update `--hex-resampling` default (`average` → `mean`), choices, help.
- `pyproject.toml` — add `exactextract`, `rasterio` to `[project.optional-dependencies].raster`.
- `Dockerfile` — no change expected; `exactextract` wheels link against system GDAL/GEOS which are present.
- `docs/raster_processing.md` — update output schema description (one row per cell) and resampling-mode docs.
- `docs/changelog.md` — note the breaking CLI change and bug fix.
- `tests/test_raster.py` — add real-data mass-conservation test.

**Create:**
- `tests/fixtures/ghs_pop_clip.tif` — small clipped tile of `ghs-pop-2020` (a few hundred KB; checked into the repo so CI is reproducible).
- `tests/fixtures/README.md` — short note on how the fixture was generated.

---

## Task 1: Add `exactextract` + `rasterio` to raster deps

**Files:**
- Modify: `pyproject.toml`
- Modify: `Dockerfile` (only if a system package is missing — verify in this task)

- [ ] **Step 1: Add deps to pyproject.toml**

Edit `pyproject.toml`, change the `[project.optional-dependencies].raster` block from:

```toml
raster = [
    "GDAL>=3.11.0",
]
```

to:

```toml
raster = [
    "GDAL>=3.11.0",
    "exactextract>=0.2.0",
    "rasterio>=1.3.0",
]
```

- [ ] **Step 2: Verify install inside the container**

The CI container is `ghcr.io/osgeo/gdal:ubuntu-full-latest`. Verify `exactextract` installs cleanly there:

```bash
docker run --rm -v "$PWD":/work -w /work ghcr.io/osgeo/gdal:ubuntu-full-latest bash -c \
  "pip install --quiet exactextract rasterio && python -c 'from exactextract import exact_extract; import rasterio; print(\"ok\")'"
```

Expected output: `ok`

If install fails, capture the error and add any required apt packages to `Dockerfile` (between the existing `apt-get install` block and the `tippecanoe` build, e.g. `libgeos-dev` if it's not already pulled in by `gdal:ubuntu-full`). The base image is `gdal-full`, so GDAL + PROJ + GEOS dev headers are typically already present.

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml Dockerfile
git commit -m "deps: add exactextract and rasterio for area-weighted h3 aggregation (#84)"
```

---

## Task 2: Generate the clipped `ghs-pop-2020` test fixture

**Files:**
- Create: `tests/fixtures/ghs_pop_clip.tif`
- Create: `tests/fixtures/README.md`

This fixture is the canonical evidence for issue #84. The clipped tile needs to be small enough to commit (<2 MB) and big enough to demonstrate the corner-effect mass loss (must contain several hundred populated h9 cells at minimum).

- [ ] **Step 1: Create the fixtures directory**

```bash
mkdir -p tests/fixtures
```

- [ ] **Step 2: Clip a small region from ghs-pop-2020**

Pick a small populated region. A 0.5° × 0.5° clip over a dense urban area (e.g., Lagos: bbox `3.0,6.3,3.6,6.9`) gives ~330×330 pixels at 90 m resolution — under 500 KB compressed as a tiled DEFLATE COG, and contains ~10–20 thousand populated h9 cells.

Run from the repo root:

```bash
gdal_translate \
  -projwin 3.0 6.9 3.6 6.3 \
  -co COMPRESS=DEFLATE \
  -co TILED=YES \
  -co BLOCKXSIZE=256 \
  -co BLOCKYSIZE=256 \
  /vsicurl/https://s3-west.nrp-nautilus.io/public-high-seas/ghs-pop-2020/ghs-pop-2020.tif \
  tests/fixtures/ghs_pop_clip.tif
```

Expected output: A geotiff written to `tests/fixtures/ghs_pop_clip.tif`. Verify file size:

```bash
ls -lh tests/fixtures/ghs_pop_clip.tif
```

Expected: `< 2 MB`. If the clip exceeds 2 MB, narrow the bbox.

- [ ] **Step 3: Record fixture provenance**

Create `tests/fixtures/README.md`:

```markdown
# Test fixtures

## `ghs_pop_clip.tif`

Clipped from the GHS-POP 2020 mosaic at:
`s3://public-high-seas/ghs-pop-2020/ghs-pop-2020.tif`

Region: bbox `3.0, 6.3, 3.6, 6.9` (Lagos, Nigeria) — chosen for high
population density (so many h9 cells contain nonzero values) within a
small (~500 KB) bounding box suitable for committing to git.

Regenerate with:

```
gdal_translate \
  -projwin 3.0 6.9 3.6 6.3 \
  -co COMPRESS=DEFLATE -co TILED=YES \
  -co BLOCKXSIZE=256 -co BLOCKYSIZE=256 \
  /vsicurl/https://s3-west.nrp-nautilus.io/public-high-seas/ghs-pop-2020/ghs-pop-2020.tif \
  tests/fixtures/ghs_pop_clip.tif
```

Used by `tests/test_raster.py::test_h3_aggregation_conserves_mass` to verify
issue #84 (raster pipeline emits one row per warped pixel, not per H3 cell).
```

- [ ] **Step 4: Verify the fixture's truth-value with rasterio**

The mass-conservation test depends on knowing the true total from the fixture. Run:

```bash
python3 -c "
import rasterio
import numpy as np
with rasterio.open('tests/fixtures/ghs_pop_clip.tif') as src:
    arr = src.read(1, masked=True)
    print(f'nodata: {src.nodata}')
    print(f'shape: {arr.shape}')
    print(f'sum:   {float(arr.sum()):.2f}')
    print(f'nonzero pixels: {int((arr > 0).sum())}')
"
```

Expected output (approximate):
```
nodata: -200.0
shape: (330, 330)
sum:   <value around 1e6 to 1e7>
nonzero pixels: <several thousand>
```

Record the `sum` value — the test asserts on it.

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/ghs_pop_clip.tif tests/fixtures/README.md
git commit -m "test: add clipped ghs-pop-2020 fixture for issue #84 mass-conservation test"
```

---

## Task 3: Write the failing mass-conservation test

**Files:**
- Test: `tests/test_raster.py` (append a new test class)

The test runs the full `RasterProcessor.process_all_h0_regions` against the fixture, reads back the output parquet partitions, and asserts that `SUM(value)` on the parquet is within 1% of the raster's true total.

- [ ] **Step 1: Add the test**

Append to `tests/test_raster.py`:

```python
class TestH3MassConservation:
    """Issue #84: SUM(value) across output parquet must equal the source raster
    total, within rounding. Pre-fix, the centroid-assignment of warped pixels
    produces a ~50% shortfall on this fixture."""

    @pytest.fixture
    def temp_dir(self):
        """Local temp directory — the fixture in TestRasterProcessor is class-scoped."""
        d = tempfile.mkdtemp()
        yield d
        shutil.rmtree(d, ignore_errors=True)

    @pytest.fixture
    def ghs_pop_clip(self):
        """Path to the committed clipped ghs-pop-2020 tile."""
        path = Path(__file__).parent / "fixtures" / "ghs_pop_clip.tif"
        if not path.exists():
            pytest.skip(f"Fixture not found: {path}")
        return str(path)

    @requires_gdal
    def test_h3_aggregation_conserves_mass(self, ghs_pop_clip, temp_dir):
        """SUM(value) over the output parquet equals the source raster SUM
        within 1%. Pre-fix this fails by ~50% for ghs-pop-2020."""
        import rasterio
        import duckdb
        from cng_datasets.raster import RasterProcessor

        # Source truth: sum of all valid pixels in the raster.
        with rasterio.open(ghs_pop_clip) as src:
            arr = src.read(1, masked=True)
            raster_sum = float(arr.sum())
            nodata = src.nodata

        assert raster_sum > 0, "Fixture must contain populated pixels"

        # Run the pipeline.
        output_dir = os.path.join(temp_dir, "ghs_pop_hex")
        processor = RasterProcessor(
            input_path=ghs_pop_clip,
            output_parquet_path=output_dir,
            h3_resolution=9,
            parent_resolutions=[0, 5, 6, 7, 8],
            value_column="population",
            hex_resampling="sum",
            nodata_value=nodata,
        )
        outputs = processor.process_all_h0_regions()
        assert len(outputs) > 0, "Pipeline produced no parquet output"

        # Read back: sum across all h0 partitions.
        con = duckdb.connect()
        parquet_glob = os.path.join(output_dir, "h0=*/data_0.parquet")
        parquet_sum = con.execute(
            f"SELECT SUM(population) FROM read_parquet('{parquet_glob}')"
        ).fetchone()[0]

        # Schema invariant: one row per h9 cell (no duplicates).
        duplicate_count = con.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT h9 FROM read_parquet('{parquet_glob}')
                GROUP BY h9 HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        assert duplicate_count == 0, (
            f"Expected one row per h9 cell, found {duplicate_count} duplicate h9 cells. "
            "Stage 2 must aggregate, not emit per-warped-pixel rows."
        )

        # Mass conservation: total within 1% of raster truth.
        relative_error = abs(parquet_sum - raster_sum) / raster_sum
        assert relative_error < 0.01, (
            f"Mass conservation violated: raster_sum={raster_sum:.2f}, "
            f"parquet_sum={parquet_sum:.2f}, relative_error={relative_error:.4f}. "
            f"Issue #84 regression."
        )
```

- [ ] **Step 2: Run the test to confirm it fails against current code**

```bash
pytest tests/test_raster.py::TestH3MassConservation::test_h3_aggregation_conserves_mass -v
```

Expected: FAIL. Likely failure mode is either `duplicate_count > 0` (one assert) or `relative_error ≈ 0.5` (second assert) — these are the two failure modes catalogued in issue #84.

If the test errors out before either assertion (e.g. import error, fixture missing), fix the test infrastructure before proceeding. Do NOT proceed to Task 4 until you have a clean FAIL on one of the two designed-for asserts.

- [ ] **Step 3: Commit**

```bash
git add tests/test_raster.py
git commit -m "test: add failing mass-conservation test for issue #84"
```

---

## Task 4: Implement `_hex_aggregate_h0` — the new Stage 2

**Files:**
- Modify: `cng_datasets/raster/cog.py` (add a new method on `RasterProcessor`, do not yet remove old code)

This method takes an h0 geometry + cell id and returns a path to the per-cell parquet output. It does NOT yet replace `process_h0_region`; we add it side-by-side in this task, then swap in Task 5.

- [ ] **Step 1: Add the new method to `RasterProcessor`**

Insert this method into `cng_datasets/raster/cog.py` immediately before `process_h0_region` (around line 706):

```python
    def _hex_aggregate_h0(self, h0_geom_wkt: str, h0_cell: int) -> Optional[str]:
        """Area-weighted aggregation of source raster into native H3 cells
        inside one h0 partition.

        Uses exactextract for fractional-pixel coverage so SUM/mean/mode
        are mass-conserving regardless of source-pixel vs hex-pitch ratio.
        Returns the output parquet path, or None if no cells produced values.
        """
        import geopandas as gpd
        import rasterio
        from shapely import wkt as shapely_wkt
        from exactextract import exact_extract

        h3_col = f"h{self.h3_resolution}"
        # Polyfill h0 → native cells, then materialize each cell's boundary.
        # The polyfill is bounded by the h0 cell, which caps the number of cells
        # per call at ~5^(h3_resolution) — comfortably millions at h9 but
        # tens-of-millions at h11. If memory becomes a concern at fine
        # resolutions, this query can be chunked by an intermediate parent;
        # not done here because current workflows top out at h9-h10.
        cells_df = self.con.execute(f"""
            WITH native_cells AS (
                SELECT UNNEST(
                    h3_polygon_wkt_to_cells('{h0_geom_wkt}', {self.h3_resolution})
                ) AS cell
            )
            SELECT
                cell AS {h3_col},
                h3_cell_to_boundary_wkt(cell) AS boundary_wkt
            FROM native_cells
        """).fetchdf()

        if len(cells_df) == 0:
            print(f"  ℹ h0 {h0_cell}: no h{self.h3_resolution} cells in polyfill")
            return None

        cells_df["geometry"] = cells_df["boundary_wkt"].apply(shapely_wkt.loads)
        gdf = gpd.GeoDataFrame(
            cells_df.drop(columns=["boundary_wkt"]),
            geometry="geometry",
            crs="EPSG:4326",
        )

        # exactextract needs the raster opened with the right nodata.
        # Open via rasterio so we can override nodata if the user passed --nodata.
        with rasterio.open(self.input_path) as rast:
            if self.nodata_value is not None and rast.nodata != self.nodata_value:
                # Build a VRT-style override by passing a masked dataset.
                # rasterio's open does not expose nodata mutation, so we
                # write a tiny sidecar VRT.
                vrt_path = f"/tmp/raster_{h0_cell}_nodata.vrt"
                gdal.Translate(
                    vrt_path,
                    self.input_path,
                    format="VRT",
                    noData=self.nodata_value,
                )
                rast_arg = vrt_path
            else:
                rast_arg = self.input_path

            results = exact_extract(
                rast=rast_arg,
                vec=gdf,
                ops=[self.hex_resampling],
                output="pandas",
                include_cols=[h3_col],
            )

        # exactextract names the output column "{band_label}_{op}". For a
        # single-band raster the label is "band_1". Normalize to value_column.
        op_col = [c for c in results.columns if c.endswith(f"_{self.hex_resampling}")]
        if not op_col:
            raise RuntimeError(
                f"exactextract returned no '{self.hex_resampling}' column; "
                f"got {list(results.columns)}"
            )
        results = results.rename(columns={op_col[0]: self.value_column})

        # Drop cells that produced no covered pixels (all-nodata under the cell).
        results = results[results[self.value_column].notna()]
        if len(results) == 0:
            print(f"  ℹ h0 {h0_cell}: no cells produced values (all nodata)")
            return None

        # Write to DuckDB to add parent columns and emit parquet.
        self.con.register("hex_values", results)

        parent_cols = []
        parent_exprs = []
        for parent_res in sorted(self.parent_resolutions):
            if parent_res < self.h3_resolution:
                col_name = f"h{parent_res}"
                parent_cols.append(col_name)
                parent_exprs.append(
                    f"h3_cell_to_parent({h3_col}, {parent_res}) AS {col_name}"
                )
        parent_sql = ", " + ", ".join(parent_exprs) if parent_exprs else ""

        output_path = (
            f"{self.output_parquet_path.rstrip('/')}/h0={h0_cell}/data_0.parquet"
        )
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        self.con.execute(f"""
            COPY (
                SELECT {self.value_column}, {h3_col}{parent_sql}
                FROM hex_values
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'zstd')
        """)
        self.con.unregister("hex_values")

        print(f"  ✓ Wrote: {output_path} ({len(results)} cells)")
        return output_path
```

- [ ] **Step 2: Verify the method compiles (no test run yet)**

```bash
python3 -c "from cng_datasets.raster.cog import RasterProcessor; print('import ok')"
```

Expected: `import ok`. If import fails, fix syntax errors before moving on.

- [ ] **Step 3: Commit (work-in-progress; old path still active)**

```bash
git add cng_datasets/raster/cog.py
git commit -m "feat: add _hex_aggregate_h0 (area-weighted h3 aggregation via exactextract)"
```

---

## Task 5: Swap `process_h0_region` to call the new aggregator

**Files:**
- Modify: `cng_datasets/raster/cog.py:706-855` — replace the body of `process_h0_region` from the start of "Extract region to XYZ using GDAL" through the `os.remove(xyz_file)` cleanup. Keep the h0 lookup + overlap-check preamble; replace everything from line 752 onward.

- [ ] **Step 1: Replace the warp+XYZ+DuckDB body with a call to `_hex_aggregate_h0`**

In `cng_datasets/raster/cog.py`, locate `process_h0_region` (line 706). Keep lines 706–750 intact (the h0 lookup, geometry fetch, overlap-check). Replace the entire block from line 752 ("Extract region to XYZ using GDAL") through line 855 (the closing `return output_path`) with:

```python
        # Area-weighted aggregation into native H3 cells (issue #84).
        # The previous gdal.Warp + XYZ + centroid-assignment path emitted
        # one row per warped pixel and lost mass at hex boundaries; replaced
        # with exact_extract over per-cell polygons.
        return self._hex_aggregate_h0(h0_geom_wkt, h0_cell)
```

After this edit, lines 706–751 (preamble: argument handling, h0 geometry fetch, overlap check) are unchanged, and line 752 onward is the single `return` above. Total `process_h0_region` body is now <50 lines.

- [ ] **Step 2: Run the mass-conservation test**

```bash
pytest tests/test_raster.py::TestH3MassConservation::test_h3_aggregation_conserves_mass -v
```

Expected: PASS.

If it fails on `duplicate_count`, the polyfill or aggregator is producing multiple rows for the same cell — debug the `cells_df` query and exact_extract's `include_cols` handling.

If it fails on `relative_error`, the issue is either (a) exactextract's nodata handling didn't pick up the value (check VRT path), (b) the polyfill is missing edge cells (h3_polygon_wkt_to_cells may exclude the boundary — try `containment_mode` if available, or expand the h0 boundary slightly), or (c) the test fixture's truth `raster_sum` was computed against pixels outside the polyfill's coverage (verify visually in QGIS).

Do NOT proceed to Task 6 until the test passes.

- [ ] **Step 3: Commit**

```bash
git add cng_datasets/raster/cog.py
git commit -m "fix: aggregate raster to native h3 cells with exactextract (#84)

Replaces the gdal.Warp -> XYZ -> centroid-assign Stage 2 path with
per-cell area-weighted aggregation via exactextract. Output is now
one row per native H3 cell with mass-conserving SUM/mean/mode."
```

---

## Task 6: Delete the dead warped-XYZ path and `_h3_res_to_degrees`

**Files:**
- Modify: `cng_datasets/raster/cog.py`

After Task 5 the warped-XYZ code is dead. Remove it and the helper that only existed to compute warp pitch.

- [ ] **Step 1: Delete `_h3_res_to_degrees`**

In `cng_datasets/raster/cog.py`, delete lines 56–70 (the `def _h3_res_to_degrees` function — `_h3_edge_length_degrees` in `vector/h3_tiling.py` is a different function used by the vector pipeline; leave that alone).

- [ ] **Step 2: Verify no other references**

```bash
grep -rn "_h3_res_to_degrees" cng_datasets/ tests/
```

Expected: no matches. If there are matches, they were missed during Stage 2 rewrite — investigate.

- [ ] **Step 3: Run the full raster test suite**

```bash
pytest tests/test_raster.py -v
```

Expected: all tests pass. If a test that doesn't use the new code fails, it was relying on the deleted code path — fix or remove.

- [ ] **Step 4: Commit**

```bash
git add cng_datasets/raster/cog.py
git commit -m "refactor: remove dead warped-XYZ pipeline and _h3_res_to_degrees helper"
```

---

## Task 7: Tighten `hex_resampling` validation (clean break)

**Files:**
- Modify: `cng_datasets/raster/cog.py` — `RasterProcessor.__init__`, around line 504 where `self.hex_resampling` is assigned.

- [ ] **Step 1: Add validation in `__init__`**

In `cng_datasets/raster/cog.py`, find the line:

```python
        self.hex_resampling = hex_resampling
```

(approximately line 504). Replace with:

```python
        # Area-weighted aggregation supports only these reducers. Old GDAL-Warp
        # values (average/near/bilinear/cubic) are rejected; #84 removed the
        # warp step entirely.
        _VALID_HEX_REDUCERS = {"sum", "mean", "mode"}
        if hex_resampling not in _VALID_HEX_REDUCERS:
            raise ValueError(
                f"hex_resampling must be one of {sorted(_VALID_HEX_REDUCERS)}, "
                f"got {hex_resampling!r}. The previous GDAL-Warp resampling "
                f"values (average, near, bilinear, cubic) are no longer supported "
                f"after #84 — see docs/raster_processing.md for migration."
            )
        self.hex_resampling = hex_resampling
```

- [ ] **Step 2: Update the docstring on `__init__`**

In the same `__init__`, locate the `hex_resampling: Resampling method ...` docstring section (around line 440–444) and replace with:

```python
            hex_resampling: Area-weighted reducer for aggregating source
                pixels into each H3 cell. One of: "sum" (counts/stocks like
                population), "mean" (intensities like NDVI), "mode" (categorical
                like land cover). Default: "mean". This replaces the older
                GDAL-Warp resampling enum (#84).
```

- [ ] **Step 3: Update the default in the signature**

In the same `__init__`, change:

```python
        hex_resampling: str = "average",
```

to:

```python
        hex_resampling: str = "mean",
```

- [ ] **Step 4: Run the test suite**

```bash
pytest tests/test_raster.py -v
```

Expected: all tests pass. The mass-conservation test passes `hex_resampling="sum"` explicitly so it's unaffected.

- [ ] **Step 5: Add a validation test**

Append to `tests/test_raster.py` in the `TestH3MassConservation` class:

```python
    @requires_gdal
    def test_hex_resampling_rejects_gdal_values(self, ghs_pop_clip, temp_dir):
        """Old GDAL-Warp resampling values must error with a clear message."""
        from cng_datasets.raster import RasterProcessor

        with pytest.raises(ValueError, match="hex_resampling must be one of"):
            RasterProcessor(
                input_path=ghs_pop_clip,
                output_parquet_path=os.path.join(temp_dir, "should_not_run"),
                h3_resolution=9,
                hex_resampling="average",
            )
```

- [ ] **Step 6: Run the new test**

```bash
pytest tests/test_raster.py::TestH3MassConservation::test_hex_resampling_rejects_gdal_values -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add cng_datasets/raster/cog.py tests/test_raster.py
git commit -m "refactor: restrict hex_resampling to sum/mean/mode (#84)

Breaking change: GDAL-Warp resampling values (average, near, bilinear,
cubic) are no longer accepted. Area-weighted aggregation supports only
sum/mean/mode. Default changed from 'average' to 'mean'."
```

---

## Task 8: Update CLI defaults, choices, and help text

**Files:**
- Modify: `cng_datasets/cli.py:44-47` — `raster` subcommand `--hex-resampling`
- Modify: `cng_datasets/cli.py:111-113` — `raster-workflow` subcommand `--hex-resampling`

- [ ] **Step 1: Update the `raster` subcommand argument**

In `cng_datasets/cli.py`, locate the lines around 44:

```python
    raster_parser.add_argument("--hex-resampling", default="average",
                               help="Resampling method for H3 hex downsampling step. "
                                    "Use 'mode' for categorical rasters (land cover, classifications); "
                                    "averaging produces meaningless values like 45 from {40, 50}.")
```

Replace with:

```python
    raster_parser.add_argument("--hex-resampling", default="mean",
                               choices=["sum", "mean", "mode"],
                               help="Area-weighted reducer for aggregating source pixels into each "
                                    "H3 cell. 'sum' for counts/stocks (population, carbon); 'mean' "
                                    "for intensities (NDVI, indices); 'mode' for categorical "
                                    "(land cover). Default: mean.")
```

- [ ] **Step 2: Update the `raster-workflow` subcommand argument**

In `cng_datasets/cli.py`, locate the lines around 111:

```python
    raster_workflow_parser.add_argument("--hex-resampling", default="average",
                                         help="Resampling method for H3 hex downsampling step. "
                                              "Use 'mode' for categorical rasters (land cover, classifications).")
```

Replace with:

```python
    raster_workflow_parser.add_argument("--hex-resampling", default="mean",
                                         choices=["sum", "mean", "mode"],
                                         help="Area-weighted reducer for H3 aggregation. 'sum' for "
                                              "counts (population, carbon); 'mean' for intensities; "
                                              "'mode' for categorical. Default: mean.")
```

- [ ] **Step 3: Verify CLI rejects old values**

```bash
python3 -m cng_datasets raster --input /tmp/nonexistent.tif --output-parquet /tmp/x --hex-resampling average 2>&1 | head -5
```

Expected: argparse error mentioning `invalid choice: 'average' (choose from 'sum', 'mean', 'mode')`.

- [ ] **Step 4: Run the CLI test suite**

```bash
pytest tests/test_cli.py -v
```

Expected: all tests pass. If a CLI test passes `--hex-resampling average` explicitly, update it to `sum`/`mean`/`mode` as appropriate (with a commit message noting the breaking change).

- [ ] **Step 5: Commit**

```bash
git add cng_datasets/cli.py tests/test_cli.py
git commit -m "feat: CLI --hex-resampling now restricted to sum/mean/mode (#84)"
```

---

## Task 9: Update docs

**Files:**
- Modify: `docs/raster_processing.md`
- Modify: `docs/changelog.md`

- [ ] **Step 1: Audit `docs/raster_processing.md` for affected content**

```bash
grep -n "hex_resampling\|hex-resampling\|average\|warp\|XYZ\|one row per\|per pixel" docs/raster_processing.md
```

Inspect each match. Sections to update:
- Any output-schema description that says "one row per pixel" or similar — replace with "one row per H3 cell".
- The `--hex-resampling` reference — list the new valid values.
- Any mention of the warped XYZ intermediate — remove.

Edit each match in turn, keeping the surrounding text intact.

- [ ] **Step 2: Add a CHANGELOG entry**

Edit `docs/changelog.md` and add at the top (under the unreleased / current-version heading):

```markdown
### Fixed
- Raster → H3 pipeline now produces one row per native H3 cell with mass-conserving
  area-weighted aggregation, fixing ~50% mass loss observed on `ghs-pop-2020` and
  `irrecoverable-carbon-2024` (#84). Implementation uses `exactextract`.

### Breaking
- `--hex-resampling` now accepts only `sum`, `mean`, or `mode` (was: any GDAL Warp
  resampling string). Default changed from `average` to `mean`. Migration: replace
  `average` with `mean`. Categorical rasters still use `mode`. New: `sum` for
  count/stock data. Rasters processed before this change have undercounted totals
  and should be reprocessed.
```

- [ ] **Step 3: Build the docs locally to confirm no broken links**

```bash
cd docs && sphinx-build -b html -W . _build/html 2>&1 | tail -20
```

Expected: `build succeeded`. If warnings/errors appear that reference your edits, fix them.

- [ ] **Step 4: Commit**

```bash
git add docs/raster_processing.md docs/changelog.md
git commit -m "docs: update raster pipeline schema and resampling docs (#84)"
```

---

## Task 10: Final verification + PR

**Files:** None (verification + PR creation)

- [ ] **Step 1: Run the full test suite**

```bash
pytest tests/ -v
```

Expected: all tests pass. Tests under `tests/test_raster.py` should include the new `TestH3MassConservation` class with both tests passing.

- [ ] **Step 2: Verify the issue's SQL reproducer no longer reproduces the bug**

The issue's reproducer relies on S3 outputs from a previous job. Translate it to the local fixture output to confirm the schema bug is gone:

```bash
python3 -c "
import duckdb
con = duckdb.connect()
parquet_glob = '/tmp/ghs_pop_hex_verify/h0=*/data_0.parquet'
# (If you don't have a /tmp run, re-run the mass-conservation test with output_dir
#  set to /tmp/ghs_pop_hex_verify so the parquet is inspectable.)
duplicates = con.execute(f'''
    SELECT COUNT(*) FROM (
        SELECT h9 FROM read_parquet(\"{parquet_glob}\")
        GROUP BY h9 HAVING COUNT(*) > 1
    )
''').fetchone()[0]
print(f'duplicate h9 cells: {duplicates}')
"
```

Expected: `duplicate h9 cells: 0`.

- [ ] **Step 3: Open the PR**

```bash
gh pr create --title "fix: area-weighted h3 aggregation for raster pipeline (#84)" --body "$(cat <<'EOF'
## Summary
- Replaces gdal.Warp → XYZ → centroid-assign Stage 2 with per-cell area-weighted aggregation via exactextract.
- One parquet row per native H3 cell; SUM(value) over the parquet equals the source raster total within rounding.
- Breaking: `--hex-resampling` now accepts only sum/mean/mode (default mean). Migration in changelog.

Closes #84.

## Test plan
- [x] New regression test asserts mass conservation against a clipped ghs-pop-2020 tile (within 1%)
- [x] New test asserts old GDAL Warp values (`average`, `near`, etc.) raise ValueError with migration hint
- [x] Full raster test suite passes
- [x] CLI test suite passes after updating affected fixtures

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL returned.

- [ ] **Step 4: Notify downstream**

Add a comment on `boettiger-lab/data-workflows#171` linking the PR, and note that any YAML configs passing `--hex-resampling average` need to be updated to `mean` (or `sum` for count rasters like ghs-pop) before re-running.
