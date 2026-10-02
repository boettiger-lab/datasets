"""
(time, lat, lon) cube → H3 hex partitions (issue #181).

One chunk — an h0 cell, or a sub-h0 cell at --chunk-resolution — per call, the
same unit of work and output layout as `cng-datasets raster`, so the workflow
generator, `merge-chunks` and every consumer treat the two alike.

Two ways to put pixels into cells, chosen by comparing their sizes:

- **aggregate** (pixels finer than cells): each pixel centre is assigned to the
  H3 cell containing it, and the cell's value is the reduction of its pixels.
  This is `warp-centroid`'s accuracy class: right for intensive variables, not
  area-weighted.
- **sample** (pixels coarser than cells): each cell reads the pixel containing
  its centre. Assigning pixels to cells here would leave most cells empty, and
  a cell lying inside one pixel *is* that pixel's value.

Both reduce along time in the same pass when --time-agg asks for it. `sum` is
refused: neither placement is area-weighted, so a summed extensive variable
would be wrong. Those still go through a 2-D COG and `exact-extract`.
"""

import math
import os
import re
from typing import List, Optional, Sequence

import numpy as np

from ..h3_chunks import (
    DEFAULT_H0_GRID_PATH,
    H3_PROTRUSION_MARGIN,
    cell_footprint,
    chunk_output_path,
    enumerate_chunk_cells,
    record_chunk_completion,
)
from ..hex_checks import assert_h3_columns_unsigned
from ..provenance import kv_metadata_sql
from .reader import CubeSource

VALID_REDUCERS = ("mean", "min", "max")
VALID_TIME_AGG = ("none", "month", "year")
VALID_PLACEMENTS = ("auto", "aggregate", "sample")

# float64 bytes per variable read together in one slab. One LOCA2 chunk
# (1952 x 123 x 139) is 267 MB at float64, so this reads it in one go.
DEFAULT_READ_BUDGET = 512 * 2**20

_KM_PER_DEG = 111.32


def _column_name(variable: str) -> str:
    name = re.sub(r"\W+", "_", variable.strip("/")).strip("_")
    if not name:
        raise ValueError(f"cannot make a column name from variable {variable!r}")
    return name


def _date_key(text: str) -> int:
    m = re.fullmatch(r"(-?\d{1,4})-(\d{1,2})-(\d{1,2})", text.strip())
    if not m:
        raise ValueError(f"expected YYYY-MM-DD, got {text!r}")
    y, mo, d = (int(g) for g in m.groups())
    return y * 10000 + mo * 100 + d


class MdimProcessor:
    """Hex one spatial chunk of a (time, lat, lon) cube into H3 cells."""

    def __init__(
        self,
        inputs: Sequence[str],
        variables: Sequence[str],
        output_parquet_path: str,
        h3_resolution: int,
        parent_resolutions: Optional[List[int]] = None,
        chunk_resolution: int = 0,
        h0_subset: Optional[List[int]] = None,
        h0_grid_path: str = DEFAULT_H0_GRID_PATH,
        hex_resampling: str = "mean",
        time_agg: str = "none",
        time_start: Optional[str] = None,
        time_end: Optional[str] = None,
        placement: str = "auto",
        read_budget_bytes: float = DEFAULT_READ_BUDGET,
    ):
        if hex_resampling == "sum":
            raise ValueError(
                "--hex-resampling sum is refused for mdim: neither placement is "
                "area-weighted, so a summed extensive variable would be wrong. "
                "Write the slice to a COG and use `cng-datasets raster` "
                "(exact-extract), which conserves mass.")
        if hex_resampling not in VALID_REDUCERS:
            raise ValueError(f"--hex-resampling must be one of {VALID_REDUCERS}, got {hex_resampling!r}")
        if time_agg not in VALID_TIME_AGG:
            raise ValueError(f"--time-agg must be one of {VALID_TIME_AGG}, got {time_agg!r}")
        if placement not in VALID_PLACEMENTS:
            raise ValueError(f"--placement must be one of {VALID_PLACEMENTS}, got {placement!r}")
        if not 0 <= chunk_resolution <= h3_resolution:
            raise ValueError("chunk_resolution must be between 0 and the H3 resolution")

        self.source = CubeSource(inputs, variables)
        self.columns = [_column_name(v) for v in variables]
        if len(set(self.columns)) != len(self.columns):
            raise ValueError(f"variables map to duplicate column names: {self.columns}")
        self.output_parquet_path = output_parquet_path
        self.h3_resolution = int(h3_resolution)
        self.parent_resolutions = sorted(
            r for r in (parent_resolutions if parent_resolutions is not None else [0])
            if r < self.h3_resolution)
        self.chunk_resolution = int(chunk_resolution)
        self.h0_subset = h0_subset
        self.h0_grid_path = h0_grid_path
        self.hex_resampling = hex_resampling
        self.time_agg = time_agg
        self.placement = placement
        self.read_budget_bytes = read_budget_bytes

        t = self.source.time
        if time_agg == "none" and t.dates is None:
            raise ValueError(
                f"calendar {t.calendar!r} has dates (e.g. 30 February) with no "
                f"calendar DATE; use --time-agg month or year, which are exact")
        key = t.year.astype(np.int64) * 10000 + t.month * 100 + t.day
        mask = np.ones(len(key), dtype=bool)
        if time_start:
            mask &= key >= _date_key(time_start)
        if time_end:
            mask &= key <= _date_key(time_end)
        idx = np.nonzero(mask)[0]
        if len(idx) == 0:
            raise ValueError(f"no time steps between {time_start} and {time_end}")
        if np.any(np.diff(key) < 0):
            raise ValueError("the time axis is not in ascending order across the inputs")
        self.t_lo, self.t_hi = int(idx[0]), int(idx[-1]) + 1
        if time_agg == "none":
            self.tkey = (t.dates.astype("datetime64[D]").astype(np.int64)).astype(np.int64)
        elif time_agg == "month":
            self.tkey = t.year.astype(np.int64) * 12 + (t.month - 1)
        else:
            self.tkey = t.year.astype(np.int64)

        from ..vector.h3_tiling import setup_duckdb_connection
        from ..storage.s3 import configure_s3_credentials
        self.con = setup_duckdb_connection()
        configure_s3_credentials(self.con)
        self._chunk_cells = None

    # -- chunks ---------------------------------------------------------

    def chunk_cells(self):
        if self._chunk_cells is None:
            self._chunk_cells = enumerate_chunk_cells(
                self.chunk_resolution, h0_subset=self.h0_subset,
                h0_grid_path=self.h0_grid_path, con=self.con)
        return self._chunk_cells

    def _footprint(self, chunk_cell: int):
        wkt = self.con.execute(
            f"SELECT h3_cell_to_boundary_wkt({int(chunk_cell)}::UBIGINT)").fetchone()[0]
        lat_lo, lat_hi, _ = cell_footprint(wkt)
        margin = H3_PROTRUSION_MARGIN * (lat_hi - lat_lo)
        return cell_footprint(wkt, margin_deg=margin)

    def resolve_placement(self, lat_centre: float) -> str:
        if self.placement != "auto":
            return self.placement
        dy, dx = self.source.pixel_deg
        pixel_km2 = dy * dx * _KM_PER_DEG ** 2 * max(math.cos(math.radians(lat_centre)), 1e-6)
        cell_km2 = self.con.execute(
            f"SELECT h3_get_hexagon_area_avg({self.h3_resolution}, 'km^2')").fetchone()[0]
        return "aggregate" if pixel_km2 < cell_km2 else "sample"

    # -- accumulation ---------------------------------------------------

    def _create_partial(self):
        cols = ", ".join(f"s_{c} DOUBLE, n_{c} BIGINT, mn_{c} DOUBLE, mx_{c} DOUBLE"
                         for c in self.columns)
        self.con.execute(f"CREATE OR REPLACE TEMP TABLE partial (cell UBIGINT, tkey BIGINT, {cols})")

    def _accumulate(self, cells: np.ndarray, t0: int, t1: int, values: List[np.ndarray]):
        """Add one slab: *values[i]* is (t1 - t0, len(cells)) for variable i."""
        import pyarrow as pa
        nt, k = t1 - t0, len(cells)
        table = {"cell": pa.array(np.tile(cells.astype(np.uint64), nt), type=pa.uint64()),
                 "tkey": pa.array(np.repeat(self.tkey[t0:t1], k), type=pa.int64())}
        for c, v in zip(self.columns, values):
            table[c] = pa.array(v.reshape(-1), type=pa.float64(), from_pandas=True)  # NaN -> NULL
        self.con.register("slab", pa.table(table))
        try:
            aggs = ", ".join(f"sum({c}), count({c}), min({c}), max({c})" for c in self.columns)
            self.con.execute(f"INSERT INTO partial SELECT cell, tkey, {aggs} FROM slab GROUP BY cell, tkey")
        finally:
            self.con.unregister("slab")

    def _aggregate_chunk(self, chunk_cell: int, windows) -> int:
        """Pixels finer than cells: pixel centre -> cell. Returns slabs read."""
        src, reads = self.source, 0
        for window in windows:
            for y0, y1, x0, x1 in src.tiles(window):
                lat = src.lat[y0:y1]
                lon = src.lon[x0:x1]
                lon = np.where(lon > 180.0, lon - 360.0, lon)
                glat, glon = np.meshgrid(lat, lon, indexing="ij")
                import pyarrow as pa
                self.con.register("pix", pa.table({
                    "p": pa.array(np.arange(glat.size, dtype=np.int64)),
                    "lat": pa.array(glat.reshape(-1)), "lon": pa.array(glon.reshape(-1))}))
                try:
                    picked = self.con.execute(f"""
                        SELECT p, cell FROM (
                            SELECT p, h3_latlng_to_cell(lat, lon, {self.h3_resolution}) AS cell FROM pix)
                        WHERE h3_cell_to_parent(cell, {self.chunk_resolution}) = {int(chunk_cell)}::UBIGINT
                        ORDER BY p
                    """).fetchnumpy()
                finally:
                    self.con.unregister("pix")
                if len(picked["p"]) == 0:
                    continue
                p = picked["p"].astype(np.int64)
                cells = picked["cell"].astype(np.uint64)
                for t0, t1 in src.time_groups(self.t_lo, self.t_hi, (y1 - y0) * (x1 - x0),
                                              self.read_budget_bytes):
                    vals = [src.read(v, t0, t1, y0, y1, x0, x1).reshape(t1 - t0, -1)[:, p]
                            for v in src.variables]
                    self._accumulate(cells, t0, t1, vals)
                    reads += 1
        return reads

    def _sample_chunk(self, chunk_cell: int) -> int:
        """Pixels coarser than cells: cell centre -> pixel. Returns slabs read."""
        src, reads = self.source, 0
        got = self.con.execute(f"""
            SELECT cell, h3_cell_to_lat(cell) AS lat, h3_cell_to_lng(cell) AS lon
            FROM (SELECT UNNEST(h3_cell_to_children({int(chunk_cell)}::UBIGINT,
                                                     {self.h3_resolution})) AS cell)
        """).fetchnumpy()
        if len(got["cell"]) == 0:
            return 0
        iy, ix = src.nearest_pixel(got["lat"], got["lon"])
        keep = (iy >= 0) & (ix >= 0)
        if not keep.any():
            return 0
        cells = got["cell"].astype(np.uint64)[keep]
        iy, ix = iy[keep], ix[keep]
        by, bx = src.block[src.axis["lat"]], src.block[src.axis["lon"]]
        tile_id = (iy // by) * (len(src.lon) // bx + 1) + (ix // bx)
        order = np.argsort(tile_id, kind="stable")
        cells, iy, ix, tile_id = cells[order], iy[order], ix[order], tile_id[order]
        bounds = np.flatnonzero(np.diff(tile_id)) + 1
        for sel in np.split(np.arange(len(cells)), bounds):
            ty, tx = iy[sel], ix[sel]
            y0, y1, x0, x1 = int(ty.min()), int(ty.max()) + 1, int(tx.min()), int(tx.max()) + 1
            for t0, t1 in src.time_groups(self.t_lo, self.t_hi, (y1 - y0) * (x1 - x0),
                                          self.read_budget_bytes):
                vals = [src.read(v, t0, t1, y0, y1, x0, x1)[:, ty - y0, tx - x0]
                        for v in src.variables]
                self._accumulate(cells[sel], t0, t1, vals)
                reads += 1
        return reads

    # -- output ---------------------------------------------------------

    def _write(self, chunk_cell: int, h0_cell: int) -> Optional[str]:
        h = f"h{self.h3_resolution}"
        parents = "".join(f", h3_cell_to_parent(cell, {r}) AS h{r}" for r in self.parent_resolutions)
        if self.time_agg == "none":
            tcols = "(DATE '1970-01-01' + tkey::INTEGER) AS time"
        elif self.time_agg == "month":
            tcols = "(tkey // 12)::INTEGER AS year, (tkey % 12 + 1)::INTEGER AS month"
        else:
            tcols = "tkey::INTEGER AS year"
        if self.hex_resampling == "mean":
            vals = ", ".join(f"sum(s_{c}) / NULLIF(sum(n_{c}), 0) AS {c}" for c in self.columns)
        else:
            fn = "min" if self.hex_resampling == "min" else "max"
            prefix = "mn" if fn == "min" else "mx"
            vals = ", ".join(f"{fn}({prefix}_{c}) AS {c}" for c in self.columns)
        has_data = " + ".join(f"sum(n_{c})" for c in self.columns)
        rows_sql = f"""
            SELECT cell AS {h}{parents}, {tcols}, {vals}
            FROM partial GROUP BY cell, tkey HAVING {has_data} > 0
            ORDER BY {h}, tkey
        """
        if self.con.execute(f"SELECT count(*) FROM ({rows_sql})").fetchone()[0] == 0:
            return None
        path = chunk_output_path(self.output_parquet_path, self.chunk_resolution,
                                 chunk_cell, h0_cell)
        if not path.startswith(("s3://", "http://", "https://")):
            os.makedirs(os.path.dirname(path), exist_ok=True)
        self.con.execute(f"COPY ({rows_sql}) TO '{path}' "
                         f"(FORMAT PARQUET, COMPRESSION 'zstd'{kv_metadata_sql()})")
        assert_h3_columns_unsigned(lambda sql: self.con.execute(sql).fetchall(), path)
        return path

    def process_chunk(self, chunk_index: int) -> Optional[str]:
        """Hex chunk *chunk_index*; returns the parquet written, or None."""
        cells = self.chunk_cells()
        if not 0 <= chunk_index < len(cells):
            raise ValueError(f"chunk index {chunk_index} is outside the {len(cells)} chunks "
                             f"at chunk-resolution {self.chunk_resolution}")
        chunk_cell, h0_cell, _ = cells[chunk_index]
        lat_lo, lat_hi, lon_intervals = self._footprint(chunk_cell)
        windows = self.source.windows(lat_lo, lat_hi, lon_intervals)
        result = None
        if windows:
            placement = self.resolve_placement((lat_lo + lat_hi) / 2.0)
            dy, dx = self.source.pixel_deg
            print(f"Chunk {chunk_index} (cell {chunk_cell}): {len(windows)} window(s), "
                  f"{self.t_hi - self.t_lo:,} time steps, {dy:g}x{dx:g} deg pixels, "
                  f"res {self.h3_resolution} -> placement '{placement}'")
            self._create_partial()
            reads = (self._aggregate_chunk(chunk_cell, windows) if placement == "aggregate"
                     else self._sample_chunk(chunk_cell))
            print(f"  {reads} slab read(s)")
            result = self._write(chunk_cell, h0_cell) if reads else None
            print(f"  ✓ Wrote {result}" if result else "  ℹ no data in this chunk")
        else:
            print(f"Chunk {chunk_index} (cell {chunk_cell}) does not overlap the source")
        if self.chunk_resolution > 0:
            record_chunk_completion(self.con, self.output_parquet_path, chunk_index,
                                    chunk_cell, h0_cell, bool(result))
        return result
