"""
Cloud-Optimized GeoTIFF (COG) creation and raster processing.

Tools for converting raster datasets to COG format and subsequently
to H3-indexed parquet files partitioned by h0 cells.
"""

from typing import Optional, Dict, List, Union
import os
import glob
import math
import shutil
import tempfile
import duckdb

from ..duckdb_memory import to_duckdb_memory_limit
from osgeo import gdal, osr
from cng_datasets.hex_checks import assert_h3_columns_unsigned
from cng_datasets.storage.s3 import configure_s3_credentials


def _split_antimeridian(geom):
    """Split a cell polygon that wraps the antimeridian into a MultiPolygon.

    h3_cell_to_boundary_wkt returns, for an H3 cell touching +/-180, a planar
    polygon whose vertices on either side of the antimeridian are joined the
    long way around — so its bounding box spans ~360 deg of longitude. Handed
    to exact_extract unchanged, such a cell integrates the entire latitude
    band rather than its true ~0.1 km^2 footprint, inflating SUM aggregates
    (issue #88). H3 cells are tiny, so a longitude span > 180 deg unambiguously
    means the cell wraps.

    We unwrap (shift negative longitudes by +360 so all vertices sit just east
    of +180), split the unwrapped polygon at x=180, and translate the eastern
    piece back by -360 — yielding two small polygons hugging +180 and -180.
    Non-wrapping geometries are returned unchanged.

    A cell touching both +/-180 and a pole unwraps to a self-intersecting ring
    near lat ~90 (all meridians converge, so the boundary spans a huge longitude
    range), which GEOS cannot intersect or union — it raises a TopologyException
    that kills the whole worker process (issue #92). We make_valid the unwrapped
    ring first (which resolves that self-intersection into the cell's true polar
    footprint), and guard the whole helper so any remaining pathological cell
    falls back to a valid copy of the original rather than crashing the worker.
    """
    minx, _, maxx, _ = geom.bounds
    if maxx - minx <= 180:
        return geom

    from shapely import make_valid
    from shapely.geometry import Polygon, box
    from shapely.affinity import translate
    from shapely.ops import unary_union
    from shapely.errors import GEOSException

    try:
        unwrapped = make_valid(Polygon([(x + 360.0 if x < 0 else x, y)
                                        for x, y in geom.exterior.coords]))
        uminx, uminy, umaxx, umaxy = unwrapped.bounds
        west = unwrapped.intersection(box(uminx, uminy, 180.0, umaxy))
        east = unwrapped.intersection(box(180.0, uminy, umaxx, umaxy))

        parts = []
        if not west.is_empty:
            parts.append(west)
        if not east.is_empty:
            parts.append(translate(east, xoff=-360.0))
        if not parts:
            return geom
        return unary_union(parts)
    except GEOSException:
        # Last resort: hand exact_extract a valid geometry so the worker
        # survives. A polar cell's true footprint is a tiny cap near +/-90,
        # where raster sources almost never have data, so even an unsplit
        # fallback contributes ~no mass.
        valid = make_valid(geom)
        return valid if not valid.is_empty else geom


def _explode_fractions(df):
    """Explode exactextract's per-cell unique/frac arrays into long rows.

    `ops=["unique", "frac"]` returns one row per cell with two parallel
    object-arrays: the distinct values present and each one's coverage-weighted
    share of the cell. The H3 partitioned-join model wants one row per
    (cell, class), so expand to a flat (_h3_str, value, frac) frame. Uses
    np.repeat/concatenate rather than DataFrame.explode — a chunk can be 100k
    cells x several classes, and the vectorized path is markedly cheaper.
    Cells with no covered pixels (entirely outside the raster footprint) carry
    empty arrays and contribute no rows.
    """
    import numpy as np
    import pandas as pd

    uniq = df["unique"].to_list()
    frac = df["frac"].to_list()
    h3 = df["_h3_str"].to_numpy()
    counts = np.fromiter(
        (0 if u is None else len(u) for u in uniq), dtype="int64", count=len(uniq)
    )
    if int(counts.sum()) == 0:
        return pd.DataFrame(
            {"_h3_str": h3[:0], "value": np.array([], dtype="float64"),
             "frac": np.array([], dtype="float64")}
        )
    vals = np.concatenate([u for u in uniq if u is not None and len(u)])
    frs = np.concatenate([f for f in frac if f is not None and len(f)])
    return pd.DataFrame(
        {"_h3_str": np.repeat(h3, counts), "value": vals, "frac": frs}
    )


# What one worker's DuckDB may hold. Small on purpose: see _worker_con.
_WORKER_DUCKDB_LIMIT = os.environ.get("CNG_HEX_WORKER_DUCKDB_LIMIT", "256MiB")

# One DuckDB connection per worker process, reused across chunks. Creating it
# per chunk would repeat an extension load thousands of times over a large h0
# (282M cells / CNG_HEX_CHUNK_SIZE); ProcessPoolExecutor reuses its processes,
# so this is created at most once per worker.
_BOUNDARY_CON = None


# A cell that exactextract found no covered pixels under carries no value, and
# the two writers say so differently: the pandas one as a null, the GDAL one as
# a float NaN. `IS NOT NULL` alone lets the NaN through — it is a value, not a
# null — and publishes cells whose value is NaN.
#
# The IEEE-754 idiom for this, `x = x`, does NOT work here: DuckDB defines
# NaN = NaN as TRUE so that NaN has a place in a total ordering. `isnan` is the
# test that means what it says, and it is safe on integer columns, where it
# returns false rather than raising (issue #173).
_IS_A_VALUE = "{col} IS NOT NULL AND NOT isnan({col})"

_OGR_PARQUET = None


def ogr_supports_parquet() -> bool:
    """Whether this GDAL build can write Parquet through OGR.

    Feature-detected, never assumed from the local environment. The runtime
    image is `ghcr.io/osgeo/gdal:ubuntu-full-latest`, which ships the Arrow and
    Parquet drivers; a distribution GDAL usually does not. Assuming either way
    is how the `cutlineWKT` gap reached a release (issue #197) — a path that
    worked in CI and raised TypeError on a developer's machine.
    """
    global _OGR_PARQUET
    if os.environ.get("CNG_HEX_GDAL_WRITER") == "0":
        # An escape hatch, so the two paths can be compared like for like and
        # a suspect result can be reproduced on the older route.
        return False
    if _OGR_PARQUET is None:
        try:
            from osgeo import ogr
            _OGR_PARQUET = ogr.GetDriverByName("Parquet") is not None
        except Exception:
            _OGR_PARQUET = False
    return _OGR_PARQUET


def _worker_con():
    """This process's DuckDB connection, created once and reused.

    Shared with `_boundary_wkt_for` because a worker process handles many
    chunks and creating a connection per chunk would repeat an extension load
    thousands of times over a large h0.
    """
    global _BOUNDARY_CON
    if _BOUNDARY_CON is None:
        con = duckdb.connect(':memory:')
        try:
            con.execute("LOAD h3")
        except duckdb.Error:
            con.execute("INSTALL h3 FROM community")
            con.execute("LOAD h3")
        # Bound it, and bound it small. This connection resolves boundaries and
        # writes one part per chunk — kilobytes of working set — but a worker
        # handles many chunks in its lifetime and DuckDB does not return its
        # buffer pool to the OS. Unbounded it sizes that pool from the host's
        # RAM, so a worker's RSS climbs with the *total* cells it has ever
        # processed rather than the chunk it is holding, which is a per-cell
        # memory term hiding in a place that looks per-chunk (issue #173).
        con.execute(f"SET memory_limit='{_WORKER_DUCKDB_LIMIT}'")
        con.execute("SET temp_directory='/tmp'")
        _BOUNDARY_CON = con
    return _BOUNDARY_CON


def _boundary_wkt_for(h3_ids):
    """
    Map H3 cell ids to their boundary WKT, in the caller's order.

    The parent used to fetch these alongside the ids and ship both to the
    worker, which made the boundary strings ~96% of a cell list materialised
    in full before any work starts — the dominant term in the parent's peak
    RSS (issue #173). A boundary is a pure function of the cell id, so each
    worker derives its own chunk's and the parent carries 8 bytes per cell
    instead of ~183.

    Uses the same `h3_cell_to_boundary_wkt` as before, so the WKT — and every
    geometry and value downstream of it — is byte-for-byte what it was.
    """
    ids = [int(h) for h in h3_ids]
    rows = _worker_con().execute(
        "SELECT cell, h3_cell_to_boundary_wkt(cell) "
        "FROM (SELECT UNNEST(?::UBIGINT[]) AS cell)",
        [ids],
    ).fetchall()
    # Paired by id rather than by position, so the result cannot depend on the
    # engine returning rows in argument order.
    wkt_by_id = dict(rows)
    return [(h, wkt_by_id[h]) for h in ids]


def _exact_extract_chunk(args):
    """Worker for chunked-parallel exact_extract over one slice of cells.

    Top-level so it pickles cleanly across processes. Receives a primitive
    array of h3 cell ids — 8 bytes each, no boundary strings, which is what
    keeps the parent's memory off the cell count (issue #173) — derives the
    boundaries for its own chunk, and returns the **path** of a parquet part
    rather than the rows themselves.

    Returning a path is what keeps the parent's memory off the cell count for
    good. exactextract can only emit pandas, GeoJSON or an OGR datasource, so
    a DataFrame is unavoidable here — but it is one chunk's worth, bounded by
    CNG_HEX_CHUNK_SIZE, and it dies with this call. What used to happen next
    was that every chunk's frame was pickled back to the parent, held in a
    list, and then `pd.concat`-ed — which allocates the result while the
    inputs are still referenced, so the parent's peak doubled at exactly its
    largest moment. Measured at ~150 bytes per cell, which on a res-10 h0 is
    ~39 GiB: essentially the whole of that job's peak.
    """
    raster_path, op_name, chunk_ids, out_dir, index = args
    if len(chunk_ids) == 0:
        return None
    cells = _boundary_wkt_for(chunk_ids)
    if ogr_supports_parquet():
        return _exact_extract_to_parquet(raster_path, op_name, cells, out_dir, index)
    frame = _exact_extract_cells(raster_path, op_name, cells)
    if frame is None or len(frame) == 0:
        return None
    return _write_chunk_part(frame, op_name, out_dir, index)


def _exact_extract_to_parquet(raster_path, op_name, chunk_cells, out_dir, index):
    """Aggregate one chunk with exactextract writing straight to disk.

    exactextract is C++ and can serialise its results through GDAL itself, so
    the rows never have to become a pandas DataFrame in this process at all.
    `include_geom` is False, so what lands on disk is the requested statistics
    and the cell id — no boundary polygons, which at 100k cells a chunk would
    dwarf the values they describe.

    DuckDB then normalises that file into the part the parent expects, in one
    streaming statement: the same `h`, `value` and (for fractions) `frac`
    columns the pandas route produces, so the two paths are
    interchangeable and the parent cannot tell which ran.
    """
    import geopandas as gpd
    from shapely import wkt as shapely_wkt
    from exactextract import exact_extract

    if not chunk_cells:
        return None
    is_fractions = op_name == "fractions"
    ops = ["unique", "frac"] if is_fractions else [op_name]

    # Split cells that straddle +/-180 into a MultiPolygon so exact_extract
    # integrates their true footprint, not a 360-deg ribbon (issue #88).
    gdf = gpd.GeoDataFrame(
        {
            "_h3_str": [str(h) for h, _ in chunk_cells],
            "geometry": [_split_antimeridian(shapely_wkt.loads(wkt))
                         for _, wkt in chunk_cells],
        },
        crs="EPSG:4326",
    )

    os.makedirs(out_dir, exist_ok=True)
    raw = os.path.join(out_dir, f"raw-{index}.parquet")
    _retry_transient_reads(
        lambda: exact_extract(
            rast=raster_path, vec=gdf, ops=ops, include_cols=["_h3_str"],
            output="gdal",
            output_options={"filename": raw, "driver": "Parquet"},
        )
    )
    if not os.path.exists(raw):
        return None

    con = _worker_con()
    try:
        columns = [r[0] for r in con.execute(
            f"SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{raw}'))"
        ).fetchall()]
        path = os.path.join(out_dir, f"part-{index}.parquet")
        if is_fractions:
            ucol = _exactextract_column(columns, "unique")
            fcol = _exactextract_column(columns, "frac")
            # The two lists are parallel per cell, and DuckDB unnests several
            # lists in one projection positionally — which is the explode that
            # used to be done with np.repeat over object arrays.
            select = (f'SELECT CAST("_h3_str" AS UBIGINT) AS h, '
                      f'UNNEST("{ucol}") AS value, UNNEST("{fcol}") AS frac '
                      f"FROM read_parquet('{raw}')")
            # frac is the float; the class value keeps whatever type the
            # source band gave it, which is what the pandas route publishes.
            keep = _IS_A_VALUE.format(col="frac")
        else:
            vcol = _exactextract_column(columns, op_name)
            # Declared, not inferred. OGR types this column from the driver's
            # own schema rules -- `mode` arrives as VARCHAR -- while the pandas
            # route has always published DOUBLE for every single-value reducer.
            # Casting here is what keeps the published schema a property of the
            # dataset rather than of which writer happened to be available.
            select = (f'SELECT CAST("_h3_str" AS UBIGINT) AS h, '
                      f'CAST("{vcol}" AS DOUBLE) AS value '
                      f"FROM read_parquet('{raw}')")
            keep = _IS_A_VALUE.format(col="value")
        con.execute(
            f"COPY (SELECT * FROM ({select}) WHERE {keep}) "
            f"TO '{path}' (FORMAT PARQUET, COMPRESSION 'zstd')"
        )
        empty = con.execute(
            f"SELECT count(*) = 0 FROM read_parquet('{path}')"
        ).fetchone()[0]
    finally:
        os.remove(raw)
    if empty:
        os.remove(path)
        return None
    return path


def _exactextract_column(columns, op_name):
    """The column exactextract used for *op_name* in this version's naming.

    Older releases emit `band_1_{op}`; >= 0.3 emits a bare `{op}` for a
    single-band raster. Resolved from the file's own schema rather than
    assumed, since the raster may be multi-band on other paths.
    """
    for column in columns:
        if column == op_name or column.endswith(f"_{op_name}"):
            return column
    raise RuntimeError(
        f"exactextract wrote no '{op_name}' column; got {columns}"
    )


def _write_chunk_part(frame, op_name, out_dir, index):
    """Write one worker's rows to a parquet part; return the path, or None.

    Columns are normalised here rather than in the parent, so the parent's
    query does not have to know which exactextract version produced them:
    `h` (UBIGINT), `value`, and `frac` for the fractions reducer. Rows that
    carry no value are dropped here too, so a part is only as large as the
    data that survives.
    """
    is_fractions = op_name == "fractions"
    if is_fractions:
        value_col = "value"
    else:
        # Older exactextract emits "band_1_{op}"; >= 0.3 emits bare "{op}" for
        # a single-band raster.
        candidates = [c for c in frame.columns
                      if c == op_name or c.endswith(f"_{op_name}")]
        if not candidates:
            raise RuntimeError(
                f"exactextract returned no '{op_name}' column; "
                f"got {list(frame.columns)}"
            )
        value_col = candidates[0]

    con = _worker_con()
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"part-{index}.parquet")
    con.register("part_frame", frame)
    try:
        # Same declared schema as the GDAL route: DOUBLE for a single-value
        # reducer, the source's own type for a fractions class code.
        value_expr = (f'"{value_col}"' if is_fractions
                      else f'CAST("{value_col}" AS DOUBLE)')
        cols = (f'CAST("_h3_str" AS UBIGINT) AS h, {value_expr} AS value'
                + (", frac" if is_fractions else ""))
        keep = _IS_A_VALUE.format(
            col="frac" if is_fractions else '"' + value_col + '"')
        con.execute(
            f"COPY (SELECT {cols} FROM part_frame WHERE {keep}) "
            f"TO '{path}' (FORMAT PARQUET, COMPRESSION 'zstd')"
        )
        empty = con.execute(
            f"SELECT count(*) = 0 FROM read_parquet('{path}')"
        ).fetchone()[0]
    finally:
        con.unregister("part_frame")
    if empty:
        os.remove(path)
        return None
    return path


def _exact_extract_cells(raster_path, op_name, chunk_cells):
    """Run exact_extract over (h3_id, boundary_wkt) pairs.

    Each worker reopens the raster itself (each process has its own
    /vsicurl/ handle and pixel cache) and returns a pandas DataFrame with the
    op output plus the cell id column as a string (the caller casts back to
    uint64).

    For the "fractions" reducer (#142) the worker requests exactextract's
    ["unique", "frac"] ops and returns the LONG (_h3_str, value, frac) shape;
    every other reducer maps to a single exactextract op and returns one row
    per cell.

    Retries transient TIFF/HTTP read failures via `_retry_transient_reads`.
    """
    import geopandas as gpd
    from shapely import wkt as shapely_wkt
    from exactextract import exact_extract

    if not chunk_cells:
        return None

    is_fractions = op_name == "fractions"
    ops = ["unique", "frac"] if is_fractions else [op_name]

    # Split cells that straddle +/-180 into a MultiPolygon so exact_extract
    # integrates their true footprint, not a 360-deg ribbon (issue #88).
    geometries = [_split_antimeridian(shapely_wkt.loads(wkt))
                  for _, wkt in chunk_cells]
    gdf = gpd.GeoDataFrame(
        {
            "_h3_str": [str(h) for h, _ in chunk_cells],
            "geometry": geometries,
        },
        crs="EPSG:4326",
    )

    def run():
        result = exact_extract(
            rast=raster_path,
            vec=gdf,
            ops=ops,
            output="pandas",
            include_cols=["_h3_str"],
        )
        if not is_fractions:
            return result
        # exactextract column naming: bare "unique"/"frac" for single-band
        # rasters, "band_1_unique"/… on older versions or multi-band.
        ucol = _exactextract_column(list(result.columns), "unique")
        fcol = _exactextract_column(list(result.columns), "frac")
        result = result.rename(columns={ucol: "unique", fcol: "frac"})
        return _explode_fractions(result)

    return _retry_transient_reads(run)


def _retry_transient_reads(call, max_attempts: int = 6):
    """Run *call*, retrying the read failures that are worth retrying.

    Ceph S3 occasionally returns a truncated tile read under heavy concurrent
    load, and GDAL surfaces that as a hard RuntimeError — which, in a worker,
    would take down the whole pool for a fault that succeeds on the next
    attempt. Shared by both aggregation paths so they cannot acquire different
    ideas about what is transient.
    """
    import time

    last_exc = None
    for attempt in range(max_attempts):
        try:
            return call()
        except RuntimeError as exc:
            msg = str(exc)
            msg_lower = msg.lower()
            transient = (
                "TIFFReadEncodedTile" in msg
                or "TIFFFillTile" in msg
                or "IReadBlock" in msg
                or "curl" in msg_lower
                or "connect" in msg_lower
                or "http" in msg_lower
                or "timed out" in msg_lower
                or "timeout" in msg_lower
            )
            if not transient or attempt == max_attempts - 1:
                raise
            last_exc = exc
            time.sleep(min(2 ** attempt, 30))  # 1, 2, 4, 8, 16, 30 seconds
    raise RuntimeError(f"Unreachable; last={last_exc}")


# How far an H3 descendant can protrude beyond its ancestor's boundary polygon,
# as a fraction of the ancestor's latitude extent. H3's hierarchy is only
# approximately containing, so a chunk's own polygon does not bound the pixels
# its native cells actually cover (issue #173).
#
# Measured over cells sampled across the globe at chunk resolutions 1-3, for
# descendants 1 to 4 levels down: the protrusion *converges* rather than
# compounding — 0.127 at one level, 0.150 by three, unchanged at four. 0.25
# carries a ~1.7x safety factor over that worst case.
#
# Two callers want different sides of the trade. Pruning a chunk uses a
# deliberately looser multiple: a false positive there costs one pod that exits
# in seconds, so generosity is nearly free. A read window pays for its margin in
# bytes on every chunk, so it uses the measured bound directly.
_H3_PROTRUSION_MARGIN = 0.25
_H3_PRUNE_MARGIN_FACTOR = 2.0

# Returned by _windowed_source_for when the chunk's window does not intersect
# the source at all. Distinct from None ("could not window, read the source
# directly"), because conflating the two turns a chunk that provably has
# nothing to contribute into one that reads the entire raster to find out.
_WINDOW_NO_OVERLAP = "__no_overlap__"

# Warn at most once per process that the CPU quota could not be read.
_CPU_QUOTA_WARNED = False

# Workers to run when the pod's CPU quota cannot be read. Deliberately small:
# see _default_hex_workers for why the host's core count is the wrong answer.
_UNKNOWN_QUOTA_HEX_WORKERS = 8


def _cgroup_cpu_count() -> Optional[int]:
    """The cgroup CPU quota (the pod's limit), or None if it cannot be read.

    Separate from what we *do* about it (`_default_hex_workers`), because the
    two are different questions: this one is a fact about the container, and
    the answer to "how many workers" is a memory decision.

    A container whose /sys/fs/cgroup is the host root rather than its own
    cgroup namespace reads cpu.max as "max" even though the pod *is* CPU
    limited, so there is no quota to be had — hence None rather than a guess
    at one (issue #195).
    """
    try:
        with open("/sys/fs/cgroup/cpu.max") as f:
            quota, period = f.read().strip().split()
            if quota != "max":
                return max(1, int(int(quota) / int(period)))
    except (FileNotFoundError, ValueError):
        pass
    try:
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") as f:
            quota = int(f.read())
        with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us") as f:
            period = int(f.read())
        if quota > 0:
            return max(1, int(quota / period))
    except (FileNotFoundError, ValueError):
        pass
    return None


# Coefficients of the peak-memory model for the hex step (issue #173):
#
#   peak  ~=  BASE  +  PER_WORKER x concurrent workers  +  PER_CELL x cells
#
# They are printed against the observed peak at the end of every aggregation
# rather than used to decide anything, so that a wrong constant is visible in
# production logs instead of being argued about. Measured to about +/-15%.
_MEM_MODEL_BASE_MIB = 250.0
_MEM_MODEL_PER_WORKER_MIB = 315.0
_MEM_MODEL_PER_CELL_BYTES = 150.0


def _peak_memory_bytes():
    """`(bytes, source)` for this container's peak memory, or None.

    Prefers the cgroup's own high-water mark: it covers the whole process tree
    and is exactly what an OOM kill is measured against. `ru_maxrss` is a poor
    substitute because RUSAGE_CHILDREN reports the largest single child rather
    than the sum of them, so it is used only to say something rather than
    nothing, and is labelled when it is.

    The cgroup figure is the *pod's* high-water mark for its whole life, not
    this step's — a pod that localized a COG first may have peaked there.
    """
    for peak_path, max_path in (
        ("/sys/fs/cgroup/memory.peak", "/sys/fs/cgroup/memory.max"),
        ("/sys/fs/cgroup/memory/memory.max_usage_in_bytes",
         "/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    ):
        try:
            with open(max_path) as f:
                limit = f.read().strip()
            # An unbounded cgroup is the host's own, and its high-water mark is
            # every process on the machine — which is how this line came to
            # report 73 GiB for a step that used 1.5. Only a bounded cgroup is
            # measuring this workload.
            if limit == "max" or int(limit) >= 2 ** 62:
                continue
            with open(peak_path) as f:
                return int(f.read().strip()), "cgroup"
        except (FileNotFoundError, ValueError, PermissionError, OSError):
            continue
    try:
        import resource
        usage = (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                 + resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
        return usage * 1024, "rusage, largest child only"
    except (ImportError, OSError):
        return None


def _report_memory_model(cells: int, workers: int) -> None:
    """Print the model's prediction beside the peak actually reached.

    Every production run is then a calibration point for the constants above,
    which is how they should have been known in the first place: the figures
    they were fitted to came from one machine and one reducer.
    """
    predicted = (_MEM_MODEL_BASE_MIB
                 + _MEM_MODEL_PER_WORKER_MIB * workers
                 + cells * _MEM_MODEL_PER_CELL_BYTES / 2 ** 20)
    observed = _peak_memory_bytes()
    line = (f"  memory: model predicts {predicted / 1024:.2f} GiB "
            f"for {cells:,} cells × {workers} workers")
    if observed is None:
        print(line)
        return
    peak_mib = observed[0] / 2 ** 20
    line += f"; peak was {peak_mib / 1024:.2f} GiB ({observed[1]})"
    if predicted > 0:
        line += f", {peak_mib / predicted:.2f}x"
    print(line)


def _default_hex_workers() -> int:
    """How many worker processes to run when CNG_HEX_WORKERS is not set.

    The pod's CPU quota when it is readable; otherwise a small constant, NOT
    the host's core count.

    This is a memory decision wearing a CPU costume. Peak RSS is roughly
    `workers x bytes-per-cell x chunk-size`, and the worker term dominates:
    measured on one LANDFIRE res-10 layer, unchanged in every other respect,
    48-64 workers peaked at 190.5 GiB and completed no slice in 3h40m, while 8
    workers peaked at ~37 GiB and completed all six with no failures (#173).

    So `os.cpu_count()` was the wrong fallback twice over. It is the *node's*
    core count inside a pod, which on a shared 256-core node meant 256 workers
    against a `cpu: 8` limit — a 32x oversubscription, and with it a peak the
    manifest never asked for (#215). And it is not even stable: two pods of one
    job reported 64 and 48 workers, so peak memory was not reproducible from
    the manifest (#195).

    The two failure modes are not symmetric. Too few workers is slower. Too
    many is an OOM kill after hours of un-checkpointed work, on a shared node,
    with nothing to resume from. When we do not know the pod's limit, the safe
    answer is the one measured to finish.

    Generated manifests have pinned CNG_HEX_WORKERS since #195, so this is the
    direct-CLI and older-manifest path rather than the usual one.
    """
    quota = _cgroup_cpu_count()
    if quota is not None:
        return quota
    workers = min(_UNKNOWN_QUOTA_HEX_WORKERS, os.cpu_count() or 1)
    global _CPU_QUOTA_WARNED
    if not _CPU_QUOTA_WARNED:
        _CPU_QUOTA_WARNED = True
        print(
            f"  ⚠ No cgroup CPU quota readable — using {workers} workers. "
            f"The host reports {os.cpu_count()} CPUs, but inside a pod that is "
            "the node's core count, not the pod's limit, and peak memory "
            "scales with workers. Set CNG_HEX_WORKERS to pin it."
        )
    return workers

# Set GDAL to use exceptions for better error handling
gdal.UseExceptions()

# Minimum proj.db schema version (DATABASE.LAYOUT.VERSION.MINOR) accepted by
# the GDAL/PROJ stack in our image (GDAL 3.13 / PROJ 9.x requires >= 7). A db
# below this throws "a number >= 7 is expected. It comes from another PROJ
# installation." for every CRS operation.
_PROJ_MIN_MINOR = 7


def _proj_db_minor(path: str) -> int:
    """Return a proj.db's DATABASE.LAYOUT.VERSION.MINOR, or -1 if unreadable."""
    import sqlite3
    try:
        conn = sqlite3.connect(path)
        try:
            row = conn.execute(
                "SELECT value FROM metadata WHERE key='DATABASE.LAYOUT.VERSION.MINOR'"
            ).fetchone()
        finally:
            conn.close()
        return int(row[0]) if row else -1
    except Exception:
        return -1


def _select_proj_db(candidates, min_minor: int = _PROJ_MIN_MINOR):
    """Pick the highest-version proj.db from `candidates`.

    Returns the path with the greatest schema MINOR version, but only if it
    meets `min_minor`; otherwise None. Selecting the maximum (rather than the
    first acceptable, as before) makes the choice deterministic regardless of
    filesystem `find` ordering, and the min_minor gate stops us from clobbering
    GDAL's own configuration with a stale db it cannot use (issue: flaky PROJ
    'number >= 7 is expected' failures in CI and cluster raster jobs).
    """
    best, best_minor = None, -1
    for path in candidates:
        minor = _proj_db_minor(path)
        if minor > best_minor:
            best, best_minor = path, minor
    return best if best_minor >= min_minor else None


_proj_configured = False


def _configure_proj():
    """Find the best PROJ database on the system and point GDAL at it.

    The container (and the CI runner) carry multiple proj.db files — a
    GDAL-compatible one and a stale Ubuntu proj-data one (MINOR == 6) that can
    take precedence. Choose the highest-version db deterministically; if none
    meets the minimum, leave GDAL's existing configuration alone rather than
    forcing a stale db.

    Lazy and idempotent: the first reprojecting entrypoint (RasterProcessor,
    create_mosaic_cog) calls this; it runs at most once per process. It used to
    run at import time, so even YAML-generation and unit-test imports paid for
    the full-filesystem `find` — slow on container overlay filesystems and the
    cause of spurious test timeouts (issue #99 fallout).

    This deliberately re-runs the deterministic `_select_proj_db` scan even when
    PROJ_DATA is already exported: the generated k8s job's bash wrapper sets
    PROJ_DATA from `find ... | head -1`, which is non-deterministic and can land
    on the stale Ubuntu db (MINOR == 6). Python's selection is authoritative and
    overrides it — trusting the pre-set value would reintroduce that race.
    """
    global _proj_configured
    if _proj_configured:
        return
    _proj_configured = True

    import subprocess

    try:
        result = subprocess.run(
            ["find", "/usr", "/opt", "/root", "-name", "proj.db"],
            capture_output=True, text=True, timeout=10
        )
        candidates = [p for p in result.stdout.strip().split("\n") if p]
        chosen = _select_proj_db(candidates)
        if chosen is not None:
            proj_dir = os.path.dirname(chosen)
            os.environ["PROJ_DATA"] = proj_dir
            os.environ["PROJ_LIB"] = proj_dir
            gdal.SetConfigOption("PROJ_DATA", proj_dir)
    except Exception:
        pass


# Reducers supported by the exact-extract H3 hex aggregator. "sum"/"mean" are
# coverage-weighted (#84); "mode" is the categorical majority; "max"/"min" are
# coverage-agnostic extrema for peak/"max-over-area" rasters like species
# richness, where sum double-counts and mean averages away the hotspot (#95).
# "fractions" is the area-accounting reducer for categorical sources (#142):
# instead of one dominant class per cell (lossy — minority classes inside a
# mixed cell are absorbed by the mode), it emits one LONG row per present
# class, (value, frac, h<res>), where frac is the class's coverage-weighted
# share of the cell. Area of class X is then the cheap hex join
# SUM(frac * cell_area). nodata is kept as an explicit class so frac sums to
# <= 1 per cell and the nodata/unclassified share is recoverable rather than
# silently inflating the real classes.
# Used both for runtime validation in RasterProcessor.__init__ and as
# argparse `choices=` in the CLI.
VALID_HEX_REDUCERS = ("sum", "mean", "mode", "max", "min", "fractions")

# Two implementations of the raster → H3 hex aggregation step.
# - "exact-extract" (default): polyfill h0 → cells, exact_extract per-cell.
#   Mass-conserving by construction; one row per cell; slow at very fine
#   resolutions because per-cell polygon coverage is exact.
# - "warp-centroid": gdal.Warp source raster to a grid at the H3 edge
#   pitch, emit one parquet row per warped pixel with its centroid mapped
#   to a hex cell. Fast and memory-light; emits N rows per hex (consumers
#   need GROUP BY h<res>); accurate only when warp pitch is finer than
#   source pitch (per the analysis in issue #84).
VALID_METHODS = ("exact-extract", "warp-centroid")


def gdal_supports_cutline_wkt() -> bool:
    """
    Whether this GDAL's Python bindings accept ``gdal.WarpOptions(cutlineWKT=)``.

    `_hex_warp_centroid_h0` clips each warp to the h0 boundary with that
    argument, so the whole warp-centroid method needs it. It is absent from
    GDAL 3.8.4 (what Ubuntu noble ships), where the call fails with a bare
    ``TypeError: WarpOptions() got an unexpected keyword argument
    'cutlineWKT'`` after the raster has already been opened — a build-time
    dependency surfacing as a mid-run crash.

    Detected by feature rather than by version number: the bindings can lag or
    lead the library, and what matters is whether this call will work.
    """
    import inspect

    try:
        return "cutlineWKT" in inspect.signature(gdal.WarpOptions).parameters
    except (TypeError, ValueError):  # pragma: no cover - exotic builds
        # Signature not introspectable; assume support and let the call speak.
        return True

# GDAL resampleAlg values accepted in warp-centroid mode. exactextract has
# a smaller vocabulary (sum / mean / mode) — warp-centroid forwards to
# gdal.Warp so it supports the full GDAL set.
VALID_WARP_RESAMPLERS = (
    "sum", "average", "mean", "near", "nearest", "bilinear",
    "cubic", "cubicspline", "lanczos", "mode", "max", "min", "med",
)

# Friendly aliases shared with the exact-extract vocabulary (and the package
# default "mean") that GDAL's resampleAlg spells differently. Canonicalize
# before handing the string to gdal.Warp, which otherwise raises
# "Unknown resampling method" — notably for the default reducer "mean".
_WARP_RESAMPLER_ALIASES = {"mean": "average", "nearest": "near"}


def _parse_nodata_values(nodata) -> List[float]:
    """Normalize a nodata specification to a list of floats.

    Accepts None, a single number, a list/tuple of numbers, or a
    comma-separated string (e.g. "-9999,-1111,32767"). Categorical raster
    products (LANDFIRE EVT/BpS/FRG, etc.) carry several fill codes —
    Fill-NoData, Fill-Not-Mapped, an internal GeoTIFF nodata — that must
    *all* be excluded; a single nodata leaves the others masquerading as
    valid classes and inflating the hex output (issue #108). Returns [] for
    None or an empty/whitespace string.
    """
    if nodata is None:
        return []
    if isinstance(nodata, (list, tuple)):
        items = list(nodata)
    elif isinstance(nodata, str):
        items = [p.strip() for p in nodata.split(",") if p.strip()]
    else:
        items = [nodata]
    return [float(x) for x in items]


def _fmt_gdal(value: float) -> str:
    """Format a nodata value for a CLI/SQL string.

    Integer-valued floats are emitted without a trailing ".0" (so a generated
    flag reads --nodata "-9999,-1111,32767" and a SQL list reads
    "Z NOT IN (-9999, 32767)"), keeping generated k8s YAML and queries clean.
    """
    f = float(value)
    return str(int(f)) if f.is_integer() else repr(f)


# Integer band types whose full value range is exactly representable as the
# doubles a VRT LUT is parsed into, so an identity entry really is the identity.
# Int64/UInt64 are deliberately absent: past 2^53 they are not, and a LUT that
# silently rounds is worse than no LUT at all.
def _compression_predictor(source_path: str) -> int:
    """2 for integer bands, 3 for float ones — GDAL rejects the wrong one.

    Not a tuning detail. On a smooth float grid PREDICTOR=3 gives 8.6 MB where
    PREDICTOR=2 gives 13.0 MB and no predictor at all gives 18.8 MB, which is
    *larger* than the 16.8 MB uncompressed file it replaces — collapsing in
    place rewrites tiles and leaves the originals behind as dead space. And
    PREDICTOR=3 on an integer band is not merely worse, it is an error, which
    would take down a run that reached the materialised path for any reason
    other than a float source.
    """
    ds = gdal.Open(source_path)
    if ds is None:
        return 2
    try:
        dtype = ds.GetRasterBand(1).DataType
    finally:
        ds = None
    return 3 if dtype in (gdal.GDT_Float32, gdal.GDT_Float64) else 2


def _lut_safe_ranges():
    ranges = {
        gdal.GDT_Byte: (0, 255),
        gdal.GDT_UInt16: (0, 65535),
        gdal.GDT_Int16: (-32768, 32767),
        gdal.GDT_UInt32: (0, 4294967295),
        gdal.GDT_Int32: (-2147483648, 2147483647),
    }
    int8 = getattr(gdal, "GDT_Int8", None)   # GDAL >= 3.7
    if int8 is not None:
        ranges[int8] = (-128, 127)
    return ranges


def _fill_collapse_lut(lo: int, hi: int, fill_values: List[float], primary: float) -> str:
    """A VRT `<LUT>` mapping every fill code to `primary` and nothing else.

    A LUT interpolates linearly between the entries it is given, so identity is
    expressed by anchoring both ends of the band's range to themselves: the
    line through (lo, lo) and (code-1, code-1) is y = x, and is exact at every
    integer along it. Each fill code then gets three entries — itself mapped to
    the primary, and its two neighbours mapped to themselves — so the only
    values the table moves are the codes, and the segments either side of a
    code contain no integers at all.
    """
    codes = sorted({int(v) for v in fill_values})
    points = {lo: lo, hi: hi}
    for code in codes:
        for neighbour in (code - 1, code + 1):
            if lo <= neighbour <= hi:
                points.setdefault(neighbour, neighbour)
    # Second pass, so a code adjacent to another code is a code, not a
    # neighbour: 0 and 1 together must both land on the primary.
    for code in codes:
        if lo <= code <= hi:
            points[code] = int(primary)
    return ",".join(f"{k}:{v}" for k, v in sorted(points.items()))


def _fill_collapse_vrt(source_path: str, fill_values: List[float],
                       primary: float, vrt_path: str):
    """A VRT view of `source_path` with every fill code collapsed to `primary`.

    Returns the path, or None when the source cannot be expressed this way and
    the caller must materialise the collapse instead.

    The materialised collapse writes `grid pixels x bytes per pixel` of
    uncompressed raster to local disk, in every pod, independent of the chunk
    that pod is working: 34 GB for a CONUS Int16 grid, which is what evicted
    every pod on the two largest layers of a LANDFIRE tranche against the 40Gi
    ephemeral limit the generator itself emits (issue #209). The mapping it
    performs is a pure per-pixel value substitution, and GDAL can express that
    as a lookup table on a ComplexSource — so it need not be pixels on disk at
    all. The VRT is a few kilobytes and GDAL applies the table on read.

    Only integer bands qualify. The identity anchors rely on there being no
    representable value between a code and its neighbours, which is false for
    floats: a float source would be silently interpolated, and quietly wrong
    values are the one outcome worse than a large temporary file.
    """
    import xml.etree.ElementTree as ET

    if any(float(v) != int(v) for v in fill_values) or float(primary) != int(primary):
        return None

    ranges = _lut_safe_ranges()
    ds = gdal.Open(source_path)
    if ds is None:
        return None
    try:
        band_ranges = []
        for b in range(1, ds.RasterCount + 1):
            rng = ranges.get(ds.GetRasterBand(b).DataType)
            if rng is None:
                return None
            if not rng[0] <= int(primary) <= rng[1]:
                # The primary has to be storable in the band it is written
                # into; a table that maps onto a value the type cannot hold
                # would be clamped on read.
                return None
            band_ranges.append(rng)
    finally:
        ds = None
    if not band_ranges:
        return None

    if gdal.Translate(vrt_path, source_path, format="VRT") is None:
        return None
    tree = ET.parse(vrt_path)
    root = tree.getroot()
    bands = root.findall("VRTRasterBand")
    if len(bands) != len(band_ranges):
        return None
    for band, (lo, hi) in zip(bands, band_ranges):
        sources = [e for e in band if e.tag in ("SimpleSource", "ComplexSource")]
        if len(sources) != 1:
            # Several sources per band means a mosaic; each would need its own
            # table, and nothing in this path produces one.
            return None
        source = sources[0]
        # A LUT is only honoured on a ComplexSource.
        source.tag = "ComplexSource"
        for nodata in source.findall("NODATA"):
            # Dropping the source-level NODATA is what lets the table see the
            # primary's own pixels; the band's NoDataValue below is what
            # exactextract actually reads.
            source.remove(nodata)
        for existing in source.findall("LUT"):
            source.remove(existing)
        ET.SubElement(source, "LUT").text = _fill_collapse_lut(
            lo, hi, fill_values, primary
        )
        for existing in band.findall("NoDataValue"):
            band.remove(existing)
        ET.SubElement(band, "NoDataValue").text = _fmt_gdal(primary)
    tree.write(vrt_path)
    return vrt_path


def _collapse_fill_values(tif_path: str, fill_values: List[float], primary: float) -> None:
    """Rewrite every fill code in `tif_path` to `primary`, in place, block-wise.

    A GDAL raster band can declare only ONE nodata value, and `srcNodata`
    with a space-separated list means *per-band* nodata, not "treat any of
    these values as nodata in one band". Categorical products (LANDFIRE)
    carry several fill codes in a single band, so collapsing them to one
    requires an explicit value remap, not a nodata declaration (issue #108).

    Reads and writes one block at a time so a continent-scale source never
    has to fit in memory, then sets the band NoData to `primary` so the
    downstream COG/hex steps exclude every former fill code via a single value.
    """
    import numpy as np

    extras = [v for v in fill_values if v != primary]
    ds = gdal.Open(tif_path, gdal.GA_Update)
    if ds is None:
        raise ValueError(f"Could not open raster to collapse fill values: {tif_path}")
    try:
        for b in range(1, ds.RasterCount + 1):
            band = ds.GetRasterBand(b)
            block_x, block_y = band.GetBlockSize()
            xsize, ysize = band.XSize, band.YSize
            for yoff in range(0, ysize, block_y):
                ny = min(block_y, ysize - yoff)
                for xoff in range(0, xsize, block_x):
                    nx = min(block_x, xsize - xoff)
                    arr = band.ReadAsArray(xoff, yoff, nx, ny)
                    if extras:
                        mask = np.isin(arr, extras)
                        if mask.any():
                            arr[mask] = primary
                            band.WriteArray(arr, xoff, yoff)
            band.SetNoDataValue(primary)
        ds.FlushCache()
    finally:
        ds = None


def _cell_footprint(geom_wkt: str, margin_deg: float = 0.0):
    """(lat_min, lat_max, [(lon_lo, lon_hi), ...]) for a cell polygon.

    Shared by the overlap test and the enumeration prune so the two cannot
    disagree about where a cell is — which, on the antimeridian, is the
    difference between pruning nothing and pruning the strip that holds the
    data (issue #88).

    Cell polygons are planar lat/lon, so a cell with vertices on both sides of
    +/-180 has a bounding box ~360 deg wide that both fails to prune anywhere
    AND wrongly excludes the +/-180 strip where its data lives. Unwrap the
    longitudes (negatives +360); a span > 180 deg means the cell straddles, so
    its longitude footprint is two intervals on [-180, 180]. Latitude is never
    wrapped, so the polygon's lat bounds are used directly.
    """
    from shapely import wkt as shapely_wkt
    poly = shapely_wkt.loads(geom_wkt)
    if poly.geom_type == "MultiPolygon":
        xs = [x for g in poly.geoms for x, _ in g.exterior.coords]
    else:
        xs = [x for x, _ in poly.exterior.coords]
    minx, miny, maxx, maxy = poly.bounds

    if maxx - minx > 180:  # straddles the antimeridian
        uxs = [x + 360.0 if x < 0 else x for x in xs]
        umin, umax = min(uxs), max(uxs)
        lon_intervals = [(umin, 180.0)]
        if umax > 180.0:
            lon_intervals.append((-180.0, umax - 360.0))
    else:
        lon_intervals = [(minx, maxx)]

    if margin_deg:
        return _widen_footprint(miny, maxy, lon_intervals, margin_deg)
    return miny, maxy, lon_intervals


def _widen_footprint(miny: float, maxy: float, lon_intervals, margin_deg: float):
    """Widen a footprint by a margin in degrees of latitude.

    Split out of `_cell_footprint` because the enumeration prune derives a
    cell's bounds in SQL rather than from parsed WKT, and the two must widen
    them identically or the prune and the overlap test disagree about the same
    cell.
    """
    miny -= margin_deg
    maxy += margin_deg
    # A degree of longitude shrinks with latitude, so a margin fixed in
    # degrees of latitude under-covers near the poles. Scale it by
    # 1/cos(lat) at the cell's furthest-from-equator edge, capped so a
    # near-polar cell widens to the whole globe rather than overflowing.
    lat = min(max(abs(miny), abs(maxy)), 89.0)
    lon_margin = min(margin_deg / max(math.cos(math.radians(lat)), 1e-6), 180.0)
    return miny, maxy, [(lo - lon_margin, hi + lon_margin) for lo, hi in lon_intervals]


def _h3_res_to_degrees(h3_resolution: int) -> float:
    """Approximate pixel size in degrees for a given H3 resolution.

    Uses the equatorial approximation (1° ≈ 111,320 m). Used only by the
    warp-centroid path to set the warp pitch to roughly one pixel per hex.
    """
    _H3_EDGE_KM = {
        0: 1281.256011, 1: 483.0568391, 2: 182.5129565, 3: 68.97922179,
        4: 26.07175968, 5: 9.854090990, 6: 3.724532667, 7: 1.406475763,
        8: 0.531414010, 9: 0.200786148, 10: 0.075863783, 11: 0.028663897,
        12: 0.010830188, 13: 0.004092010, 14: 0.001546100, 15: 0.000584169,
    }
    return _H3_EDGE_KM[h3_resolution] * 1000 / 111320.0


def _ensure_vsi_path(path: str, use_public_endpoint: bool = False) -> str:
    """Convert path to appropriate GDAL VSI notation.

    Args:
        path: Input path (s3://, https://, or local)
        use_public_endpoint: If True, convert s3:// to /vsicurl/ with public HTTPS URL
                            for single-file reads (faster for public data)

    Returns:
        Path in GDAL VSI notation
    """
    if path.startswith("s3://"):
        if use_public_endpoint:
            # Use public HTTPS endpoint with /vsicurl/ for single file reads
            bucket_path = path[5:]  # Remove s3://
            # Never hardwire endpoint - respect AWS_PUBLIC_ENDPOINT or AWS_S3_ENDPOINT env var
            endpoint = os.getenv('AWS_PUBLIC_ENDPOINT', os.getenv('AWS_S3_ENDPOINT', 's3-west.nrp-nautilus.io'))
            # Determine protocol from AWS_HTTPS env var (default TRUE for public endpoint)
            use_ssl = os.getenv('AWS_HTTPS', 'TRUE').upper() != 'FALSE'
            protocol = 'https' if use_ssl else 'http'
            return f"/vsicurl/{protocol}://{endpoint}/{bucket_path}"
        else:
            # Use /vsis3/ for writes and multi-file operations
            return f"/vsis3/{path[5:]}"
    if path.startswith("https://") or path.startswith("http://"):
        return f"/vsicurl/{path}"
    return path


DEFAULT_H0_GRID_PATH = "s3://public-grids/hex/h0-valid.parquet"


def describe_h0(h0_cell: int, con) -> str:
    """"<cell id> (h3 <string>, H3 base cell N)" — for start-up logging.

    A mis-specified `--h0-subset` is otherwise invisible until the finished
    product's extent is measured, because the positions and the H3 base cell
    numbers occupy the same 0-121 range and a wrong value is always in it
    (issue #213). Printing what a position actually resolved to puts the
    mistake in the first lines of the log instead.
    """
    try:
        as_str, base = con.execute(
            f"SELECT h3_h3_to_string({int(h0_cell)}::ubigint), "
            f"h3_get_base_cell_number({int(h0_cell)}::ubigint)"
        ).fetchone()
        return f"{h0_cell} (h3 {as_str}, H3 base cell {base})"
    except duckdb.Error:
        return str(h0_cell)


def h0_positions_for_base_cells(base_cells: List[int],
                                h0_grid_path: str = DEFAULT_H0_GRID_PATH,
                                con=None) -> List[int]:
    """Grid positions for a list of H3 **base cell numbers**.

    `--h0-index` and `--h0-subset` are positions in the h0 grid's own `i`
    column, which is an arbitrary permutation of the 122 base cells — `i = 12`
    is base cell 9, and exactly one of the 122 positions coincides with its
    base cell. Both numberings run 0-121, so a base-cell list passed as
    positions is always in range, never errors, and builds a different part of
    the world (issue #213). This is the conversion that makes the list a user
    computed from the H3 library usable, rather than plausible-looking and
    wrong.
    """
    wanted = sorted({int(b) for b in base_cells})
    out_of_range = [b for b in wanted if not 0 <= b <= 121]
    if out_of_range:
        raise ValueError(
            f"H3 base cell numbers run 0-121; got {out_of_range}."
        )

    own_con = con is None
    if own_con:
        con = duckdb.connect(":memory:")
        try:
            con.execute("LOAD h3")
        except duckdb.Error:
            con.execute("INSTALL h3 FROM community")
            con.execute("LOAD h3")
        configure_s3_credentials(con)
    try:
        rows = con.execute(f"""
            SELECT h3_get_base_cell_number(h0::ubigint) AS base, i
            FROM read_parquet('{h0_grid_path}')
            WHERE base IN ({', '.join(str(b) for b in wanted)})
            ORDER BY i
        """).fetchall()
    finally:
        if own_con:
            con.close()

    found = {int(base) for base, _ in rows}
    missing = [b for b in wanted if b not in found]
    if missing:
        raise ValueError(
            f"H3 base cells {missing} are not in the h0 grid at {h0_grid_path}."
        )
    return [int(i) for _, i in rows]


def enumerate_chunk_cells(chunk_resolution: int,
                          h0_subset: Optional[List[int]] = None,
                          h0_grid_path: str = DEFAULT_H0_GRID_PATH,
                          con=None):
    """Ordered [(chunk_cell, h0_cell, h0_index)] — the units of work for a build.

    Shared by `RasterProcessor` and the workflow generator on purpose. The
    generator sizes a Job's completions from this list and a pod indexes into
    it; if the two computed it separately they could disagree, and a fan-out
    narrower than the list drops whole chunks with nothing to show for it.

    Ordering is (h0 index, chunk cell id) — deterministic and independent of the
    order h3_cell_to_children happens to return, so an index maps to the same
    cell across regenerations and DuckDB versions.

    At chunk_resolution 0 this is the h0 grid itself, in grid order, so the
    index -> cell mapping is identical to the historical --h0-index.
    """
    if chunk_resolution < 0:
        raise ValueError(f"chunk_resolution must be >= 0, got {chunk_resolution}")

    own_con = con is None
    if own_con:
        con = duckdb.connect(":memory:")
        try:
            con.execute("LOAD h3")
        except duckdb.Error:
            con.execute("INSTALL h3 FROM community")
            con.execute("LOAD h3")
        configure_s3_credentials(con)

    try:
        where_sql = ""
        if h0_subset:
            where_sql = f"WHERE i IN ({', '.join(str(int(h)) for h in sorted(set(h0_subset)))})"

        base = con.execute(f"""
            SELECT i, h0
            FROM read_parquet('{h0_grid_path}')
            {where_sql}
            ORDER BY i
        """).fetchall()

        if chunk_resolution == 0:
            return [(int(h0), int(h0), int(i)) for i, h0 in base]

        # Expanded one h0 at a time with the cell as a literal. The obvious
        # single query — UNNEST(h3_cell_to_children(h0, N)) selected alongside
        # i and h0 over a parquet scan — is not merely slow, it aborts DuckDB
        # with "INTERNAL Error: Calling GetValueInternal on a value that is
        # NULL" and invalidates the connection, so every later query in the
        # process fails too. The literal form is the one already proven in
        # _native_cells_for_h0, and there are at most 122 of these.
        cells = []
        for i, h0 in base:
            children = con.execute(
                f"SELECT UNNEST(h3_cell_to_children({int(h0)}, {chunk_resolution}))"
            ).fetchall()
            cells.extend((int(child), int(h0), int(i)) for (child,) in children)
        cells.sort(key=lambda row: (row[2], row[0]))
        return cells
    finally:
        if own_con:
            con.close()


def _localize_input(input_path: str, cache_dir: str) -> str:
    """Copy a remote raster (s3:// or http(s)://) to local disk and return the local path.

    The downstream exact_extract pipeline does many small range reads against
    the source raster while integrating fractional pixel coverage per H3
    cell. When the source is remote, each read pays HTTP round-trip latency,
    and on dense h0 cells workers spend the majority of wall time blocked
    on I/O — empirically ~12× slower than reading from a local NVMe copy.

    For local input paths this is a no-op (returns input_path unchanged).
    For remote inputs, uses rclone if available (matches the NRP/Ceph
    production pattern, respects rclone-config), otherwise falls back to
    GDAL's VSI layer (gdal.VSICopyFile) which uses the same credential/
    endpoint env vars the rest of the package already honors.
    """
    import shutil
    import subprocess

    if not (input_path.startswith("s3://")
            or input_path.startswith("http://")
            or input_path.startswith("https://")):
        return input_path  # already local

    os.makedirs(cache_dir, exist_ok=True)
    basename = os.path.basename(input_path.rstrip("/"))
    local_path = os.path.join(cache_dir, basename)

    if os.path.exists(local_path):
        print(f"✓ Local cache hit: {local_path}")
        return local_path

    print(f"  Localizing input → {local_path}")

    # Prefer rclone — it's what production NRP jobs use and handles
    # /vsis3-style endpoints via the configured remote. rclone respects
    # the rclone-config secret typically mounted into NRP pods.
    if shutil.which("rclone"):
        try:
            # Convert s3://bucket/path → nrp:bucket/path for the standard
            # NRP remote name. Users with a different remote can pre-stage
            # the file or call with a local path.
            if input_path.startswith("s3://"):
                rclone_src = "nrp:" + input_path[len("s3://"):]
            else:
                rclone_src = input_path
            cmd = ["rclone", "copy", "--s3-no-check-bucket",
                   "--transfers", "4", rclone_src, cache_dir]
            subprocess.run(cmd, check=True)
            if os.path.exists(local_path):
                print(f"  ✓ Localized via rclone: {os.path.getsize(local_path)} bytes")
                return local_path
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            print(f"  ⚠ rclone copy failed ({e}); falling back to GDAL VSI copy")

    # Fallback: stream the bytes through GDAL VSI to a local file. This
    # respects AWS_S3_ENDPOINT / AWS_ACCESS_KEY_ID / AWS_HTTPS env vars
    # that configure_s3_credentials sets up for the rest of the package.
    vsi_src = _ensure_vsi_path(input_path, use_public_endpoint=False)
    src_fh = gdal.VSIFOpenL(vsi_src, "rb")
    if src_fh is None:
        raise RuntimeError(f"Could not open remote source for localization: {vsi_src}")
    try:
        with open(local_path, "wb") as dst:
            while True:
                buf = gdal.VSIFReadL(1, 16 * 1024 * 1024, src_fh)  # 16 MiB
                if not buf:
                    break
                dst.write(buf)
    finally:
        gdal.VSIFCloseL(src_fh)

    print(f"  ✓ Localized via GDAL VSI: {os.path.getsize(local_path)} bytes")
    return local_path


def is_cog(url: str) -> bool:
    """Check if a raster is a Cloud-Optimized GeoTIFF.

    Returns True if the raster has tiled blocks and internal overviews (COG structure).
    Returns True (fail-safe) if the file cannot be opened — avoids unnecessary preprocess
    steps when network access is unavailable at workflow-generation time.
    Returns False only when the file is confirmed to be non-COG.

    Args:
        url: Path to raster file (s3://, https://, or local path).

    Returns:
        True if COG (or unverifiable), False if confirmed non-COG.
    """
    try:
        vsi_path = _ensure_vsi_path(url, use_public_endpoint=True)
        ds = gdal.Open(vsi_path)
        if ds is None:
            return True  # Can't check — assume COG
        band = ds.GetRasterBand(1)
        # Must have internal overviews
        if band.GetOverviewCount() == 0:
            return False
        # Must have tiled (not stripped) blocks
        block_x, _ = band.GetBlockSize()
        if block_x == ds.RasterXSize:  # stripped layout
            return False
        return True
    except Exception:
        return True  # Can't check — assume COG


def band_count(raster_path: str) -> int:
    """How many bands *raster_path* has."""
    ds = gdal.Open(raster_path)
    if ds is None:
        raise ValueError(f"Could not open input raster: {raster_path}")
    count = ds.RasterCount
    ds = None
    return count


def band_subset_vrt(raster_path: str, band: int) -> str:
    """A single-band VRT view of *raster_path*, selecting *band* (1-indexed).

    Selecting the band once, at the source, is what keeps the rest of the
    pipeline honest: nodata detection, the windowed read, the fill-code
    collapse, exactextract and the warp all then see a raster with exactly one
    band and no way to pick the wrong one. A VRT rather than a copy because it
    costs nothing and defers every read to the original.
    """
    count = band_count(raster_path)
    if not 1 <= band <= count:
        raise ValueError(
            f"--band {band} is out of range: {raster_path} has {count} "
            f"band{'' if count == 1 else 's'}, numbered 1-{count}."
        )
    vrt_dir = tempfile.mkdtemp(prefix="cng_band_")
    vrt_path = os.path.join(vrt_dir, f"band{band}.vrt")
    result = gdal.Translate(vrt_path, raster_path, format="VRT", bandList=[band])
    if result is None:
        raise RuntimeError(
            f"Could not select band {band} of {raster_path}: {gdal.GetLastErrorMsg()}"
        )
    result = None
    return vrt_path


def assert_band_is_unambiguous(raster_path: str, band: Optional[int]) -> None:
    """Refuse to hex a multi-band raster that has not said which band it means.

    exactextract defaults to the first band, the output column is named by
    --value-column whichever band it came from, and both bands of a stacked
    product usually share a value range — so a wrong-band build is
    indistinguishable from a right one without re-measuring against the source.
    That is how 384,922,346 rows of annual grass cover were published and
    documented as perennial (issue #214). A job that refuses at submission is
    the cheap failure; a dataset built from the wrong band is the expensive one.
    """
    if band is not None:
        return
    count = band_count(raster_path)
    if count > 1:
        raise ValueError(
            f"Input raster has {count} bands and no band was selected, so which "
            f"one to hex is ambiguous (issue #214).\n"
            f"  The first band would be read silently, and the output column "
            f"would carry the --value-column name either way.\n"
            f"  Pass --band N (1-indexed) to choose one, or subset the band "
            f"first:\n"
            f"    gdal_translate -b N {raster_path} single-band.tif"
        )


def detect_nodata_value(raster_path: str, verbose: bool = True) -> Optional[float]:
    """
    Detect NoData value from raster metadata.

    Args:
        raster_path: Path to raster file (can be /vsis3/ URL)
        verbose: Whether to print detection message (default: True)

    Returns:
        NoData value if found, None otherwise
    """
    # Use internal endpoint so this works both inside and outside the cluster
    raster_path = _ensure_vsi_path(raster_path, use_public_endpoint=False)
    ds = gdal.Open(raster_path)
    if ds is None:
        raise ValueError(f"Could not open raster: {raster_path}")

    # Get the first band
    band = ds.GetRasterBand(1)
    nodata_value = band.GetNoDataValue()

    ds = None

    if nodata_value is not None and verbose:
        print(f"✓ Auto-detected NoData value: {nodata_value}")
    elif verbose:
        print("ℹ No NoData value found in raster metadata")

    return nodata_value


def detect_optimal_h3_resolution(raster_path: str, verbose: bool = True) -> int:
    """
    Detect optimal H3 resolution based on raster resolution.

    Uses the finest pixel dimension to recommend an H3 resolution.
    H3 average edge lengths (from https://h3geo.org/docs/core-library/restable):
    - h15: 0.58m, h14: 1.5m, h13: 4.1m, h12: 10.8m, h11: 28.7m
    - h10: 75.9m, h9: 200.8m, h8: 531.4m, h7: 1.4km, h6: 3.7km
    - h5: 9.9km, h4: 26.1km, h3: 69.0km, h2: 182.5km, h1: 483.1km, h0: 1281.3km

    Args:
        raster_path: Path to raster file (can be /vsis3/ URL)
        verbose: Whether to print detection message (default: True)

    Returns:
        Recommended H3 resolution (0-15)
    """
    # Use internal endpoint so this works both inside and outside the cluster
    raster_path = _ensure_vsi_path(raster_path, use_public_endpoint=False)
    ds = gdal.Open(raster_path)
    if ds is None:
        raise ValueError(f"Could not open raster: {raster_path}")

    # Get geotransform to compute resolution
    gt = ds.GetGeoTransform()
    pixel_width = abs(gt[1])
    pixel_height = abs(gt[5])

    # Use finest resolution
    pixel_res_deg = min(pixel_width, pixel_height)

    # Convert to meters (approximate at equator: 1 degree ≈ 111km)
    pixel_res_m = pixel_res_deg * 111000

    ds = None

    # Map to H3 resolution
    # Use ~3x pixel resolution as target H3 edge length
    target_edge_m = pixel_res_m * 3

    # H3 average edge lengths in Km (from https://h3geo.org/docs/core-library/restable/)
    # Converting to meters for comparison
    h3_edge_lengths_km = {
        0: 1281.256011, 1: 483.0568391, 2: 182.5129565, 3: 68.97922179,
        4: 26.07175968, 5: 9.854090990, 6: 3.724532667, 7: 1.406475763,
        8: 0.531414010, 9: 0.200786148, 10: 0.075863783, 11: 0.028663897,
        12: 0.010830188, 13: 0.004092010, 14: 0.001546100, 15: 0.000584169
    }
    h3_edge_lengths = {res: km * 1000 for res, km in h3_edge_lengths_km.items()}

    # Find closest H3 resolution
    best_res = 8  # default
    min_diff = float('inf')

    for res, edge_m in h3_edge_lengths.items():
        diff = abs(math.log10(edge_m) - math.log10(target_edge_m))
        if diff < min_diff:
            min_diff = diff
            best_res = res

    if verbose:
        print(f"Raster resolution: {pixel_res_m:.1f}m → Recommended H3: {best_res}")
    return best_res


# The catalog's universal join key. A dataset whose finest H3 resolution is
# coarser than this carries no h8 column at all, so it cannot be joined against
# the rest of the catalog on h8 (issue #182).
CATALOG_JOIN_RESOLUTION = 8


def h3_resolution_join_warning(
    resolution: int,
    user_specified: bool,
    parent_resolutions: Optional[List[int]] = None,
) -> Optional[str]:
    """
    Build the warning text when a build carries no path to the h8 join key.

    Two ways to lose it, both silent today (issue #182):

    * **Target too coarse.** `detect_optimal_h3_resolution` targets ~3x the
      source pixel edge, which agrees with the catalog convention at fine pixels
      and diverges as pixels coarsen — a ~1 km global raster auto-detects h6,
      two levels below h8. Detection is only a suggestion, so a caller who omits
      ``--h3-resolution`` gets a non-joinable dataset with no error, only an
      informational log line.
    * **h8 not among the parents.** A finer target only carries h8 if h8 is a
      requested parent resolution, and the default is ``--parent-resolutions 0``
      — so an h10 build emits h10 and h0 and nothing to join on.

    Args:
        resolution: The H3 resolution the build will actually use
        user_specified: Whether the caller passed the resolution explicitly
        parent_resolutions: Parent resolutions the build will emit. None skips
            the parent check, for callers that do not know them yet.

    Returns:
        Warning text, or None when the build can join on h8
    """
    if resolution == CATALOG_JOIN_RESOLUTION:
        return None

    if resolution > CATALOG_JOIN_RESOLUTION:
        if parent_resolutions is None:
            return None
        if CATALOG_JOIN_RESOLUTION in parent_resolutions:
            return None
        emitted = ", ".join(
            f"h{r}" for r in [resolution] + sorted(parent_resolutions, reverse=True)
        )
        return (
            f"⚠ This build emits {emitted} — no "
            f"h{CATALOG_JOIN_RESOLUTION} column.\n"
            f"  h{CATALOG_JOIN_RESOLUTION} is the catalog's universal join key, "
            f"so the output cannot be joined against the\n"
            f"  rest of the catalog. Add {CATALOG_JOIN_RESOLUTION} to "
            f"--parent-resolutions to emit it."
        )

    consequence = (
        f"  A dataset built at h{resolution} carries no "
        f"h{CATALOG_JOIN_RESOLUTION} column, so it cannot be joined against "
        f"the rest of the catalog\n"
        f"  on h{CATALOG_JOIN_RESOLUTION} — the universal join key.\n"
    )

    if user_specified:
        return (
            f"⚠ Using h{resolution}, coarser than the "
            f"h{CATALOG_JOIN_RESOLUTION} catalog join key.\n"
            f"{consequence}"
            f"  Proceeding: the resolution was specified explicitly."
        )

    return (
        f"⚠ Auto-detected h{resolution} is coarser than the "
        f"h{CATALOG_JOIN_RESOLUTION} catalog join key.\n"
        f"{consequence}"
        f"  Auto-detection targets ~3x the source pixel edge (~9 pixels per "
        f"cell), which is coarser than the\n"
        f"  catalog convention of roughly one cell per pixel at this pixel "
        f"size.\n"
        f"  Pass --h3-resolution {CATALOG_JOIN_RESOLUTION} to build a "
        f"joinable dataset, or --h3-resolution {resolution} to make this "
        f"coarse\n"
        f"  build a deliberate choice."
    )


def create_mosaic_cog(
    source_urls: List[str],
    output_path: str,
    target_crs: str = "EPSG:4326",
    target_extent: Optional[tuple] = None,
    target_resolution: Optional[float] = None,
    band: Optional[int] = None,
    nodata: Optional[Union[float, str, List[float]]] = None,
    resampling: str = "bilinear",
    compression: str = "deflate",
    overview_resampling: str = "average",
) -> str:
    """
    Mosaic multiple raster tiles (potentially in different CRS) into a single COG.

    Handles the common case where source data is distributed as per-UTM-zone tiles
    (e.g. RAP 10m products across zones 12 and 13 for Wyoming). Groups tiles by CRS,
    warps each group to the target CRS, merges, then writes a Cloud-Optimized GeoTIFF.

    Args:
        source_urls: List of tile paths/URLs (local, /vsicurl/, s3://).
                     Tiles may be in mixed CRS (e.g. multiple UTM zones).
        output_path: Destination path for the COG (local path or s3://).
        target_crs: Output CRS (default: EPSG:4326).
        target_extent: Clip extent as (xmin, ymin, xmax, ymax) in target_crs.
                       If None, uses the union of all tile extents.
        target_resolution: Output pixel size in target_crs units (e.g. 0.0001 for ~10m
                           in degrees). If None, derived from the finest source tile.
        band: Extract a single band from multi-band sources (1-indexed). If None,
              all bands are preserved.
        nodata: NoData value(s) for output. Accepts a single number, a list, or
                a comma-separated string ("-9999,-1111,32767"); all are applied
                as srcNodata and collapsed to the first as dstNodata so multi-fill
                categorical products end up with one nodata (issue #108). If None,
                inherited from source tiles.
        resampling: Resampling algorithm for warping (default: bilinear).
        compression: COG compression (deflate, lzw, zstd).
        overview_resampling: Resampling for COG overviews. Use "mode"/"nearest"
                for categorical sources — "average" corrupts class codes.

    Returns:
        output_path (echoed back for chaining)
    """
    if not source_urls:
        raise ValueError("source_urls must not be empty")

    nodata_values = _parse_nodata_values(nodata)

    _configure_proj()
    print(f"Creating mosaic COG from {len(source_urls)} source tile(s)...")

    # Resolve VSI paths for all sources
    vsi_urls = [_ensure_vsi_path(u, use_public_endpoint=True) for u in source_urls]

    # Group tiles by their CRS authority string (e.g. "EPSG:32612")
    crs_groups: dict = {}
    for vsi_url in vsi_urls:
        ds = gdal.Open(vsi_url)
        if ds is None:
            print(f"  ⚠ Could not open {vsi_url}, skipping")
            continue
        srs = osr.SpatialReference(wkt=ds.GetProjection())
        try:
            srs.AutoIdentifyEPSG()
        except RuntimeError:
            pass  # no EPSG authority code (e.g. ESRI:102003); proceed with WKT/PROJ definition
        epsg = srs.GetAuthorityCode(None)
        crs_key = f"EPSG:{epsg}" if epsg else srs.ExportToProj4()
        ds = None
        crs_groups.setdefault(crs_key, []).append(vsi_url)

    if not crs_groups:
        raise RuntimeError("No readable source tiles found")

    print(f"  CRS groups: { {k: len(v) for k, v in crs_groups.items()} }")

    workdir = tempfile.mkdtemp(prefix="mosaic_cog_")
    try:
        warped_paths = []

        warp_kwargs = dict(
            dstSRS=target_crs,
            resampleAlg=resampling,
            multithread=True,
            format="GTiff",
            creationOptions=["COMPRESS=NONE", "BIGTIFF=IF_SAFER"],
        )
        if target_extent is not None:
            xmin, ymin, xmax, ymax = target_extent
            warp_kwargs["outputBounds"] = (xmin, ymin, xmax, ymax)
            warp_kwargs["outputBoundsSRS"] = target_crs
        if target_resolution is not None:
            warp_kwargs["xRes"] = target_resolution
            warp_kwargs["yRes"] = target_resolution
        primary_nodata = nodata_values[0] if nodata_values else None
        if primary_nodata is not None:
            # A band carries one nodata value; declare the primary here and
            # remap any remaining fill codes to it after the merge (issue #108).
            warp_kwargs["srcNodata"] = _fmt_gdal(primary_nodata)
            warp_kwargs["dstNodata"] = primary_nodata

        for i, (crs_key, tiles) in enumerate(crs_groups.items()):
            print(f"  Building VRT for {crs_key} ({len(tiles)} tiles)...")
            vrt_path = os.path.join(workdir, f"group_{i}.vrt")
            vrt_ds = gdal.BuildVRT(vrt_path, tiles, bandList=[band] if band else None)
            if vrt_ds is None:
                raise RuntimeError(f"gdal.BuildVRT failed for CRS group {crs_key}")
            vrt_ds = None  # flush

            warped_path = os.path.join(workdir, f"warped_{i}.tif")
            print(f"  Warping {crs_key} → {target_crs}...")
            result = gdal.Warp(warped_path, vrt_path, **warp_kwargs)
            if result is None:
                raise RuntimeError(f"gdal.Warp failed for CRS group {crs_key}: {gdal.GetLastErrorMsg()}")
            result = None
            warped_paths.append(warped_path)

        # Merge all warped groups into a final VRT
        print(f"  Merging {len(warped_paths)} warped group(s)...")
        merged_vrt = os.path.join(workdir, "merged.vrt")
        build_vrt_opts = {}
        if primary_nodata is not None:
            build_vrt_opts["srcNodata"] = primary_nodata
            build_vrt_opts["VRTNodata"] = primary_nodata
        merged_ds = gdal.BuildVRT(merged_vrt, warped_paths, **build_vrt_opts)
        if merged_ds is None:
            raise RuntimeError(f"gdal.BuildVRT failed for merge: {gdal.GetLastErrorMsg()}")
        merged_ds = None

        # Write intermediate GTiff, build overviews, then write final COG.
        # Writing directly to COG via auto-overview generation is unreliable for S3
        # output and for large files; the explicit BuildOverviews + COPY_SRC_OVERVIEWS
        # pattern is the recommended approach for fast GDAL downsampling (issue #25).
        print("  Writing intermediate GTiff...")
        tmp_tif = os.path.join(workdir, "intermediate.tif")
        tmp_opts = gdal.TranslateOptions(
            format="GTiff",
            creationOptions=["COMPRESS=NONE", "BIGTIFF=IF_SAFER", "NUM_THREADS=ALL_CPUS"],
        )
        result = gdal.Translate(tmp_tif, merged_vrt, options=tmp_opts)
        if result is None:
            raise RuntimeError(f"gdal.Translate (GTiff) failed: {gdal.GetLastErrorMsg()}")
        result = None

        # Collapse any secondary fill codes to the primary nodata before
        # overviews so the merged COG carries a single nodata (issue #108).
        if len(nodata_values) > 1:
            print(f"  Collapsing fill codes {nodata_values} → {primary_nodata}...")
            _collapse_fill_values(tmp_tif, nodata_values, primary_nodata)

        print("  Building overviews...")
        tmp_ds = gdal.Open(tmp_tif, gdal.GA_Update)
        tmp_ds.BuildOverviews(overview_resampling.upper(), [2, 4, 8, 16, 32, 64])
        tmp_ds = None

        print(f"  Writing COG → {output_path}...")
        cog_output = _ensure_vsi_path(output_path)
        # COG driver requires random-write access; /vsis3/ needs this config option.
        if cog_output.startswith("/vsis3/"):
            gdal.SetConfigOption("CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE", "YES")
        translate_opts = gdal.TranslateOptions(
            format="COG",
            creationOptions=[
                f"COMPRESS={compression.upper()}",
                "PREDICTOR=YES",
                "COPY_SRC_OVERVIEWS=YES",
                f"OVERVIEW_RESAMPLING={overview_resampling.upper()}",
                "BIGTIFF=IF_SAFER",
                "NUM_THREADS=ALL_CPUS",
            ],
        )
        result = gdal.Translate(cog_output, tmp_tif, options=translate_opts)
        if result is None:
            raise RuntimeError(f"gdal.Translate (COG) failed: {gdal.GetLastErrorMsg()}")
        result = None

        print(f"  ✓ Mosaic COG created: {output_path}")
        return output_path

    finally:
        import shutil
        shutil.rmtree(workdir, ignore_errors=True)


class RasterProcessor:
    """
    Process raster datasets into cloud-native formats.

    Converts raster data to COG format and H3-indexed parquet files
    partitioned by h0 cells, processing each h0 region separately
    for memory efficiency with global datasets.
    """

    def __init__(
        self,
        input_path: Union[str, List[str]],
        output_cog_path: Optional[str] = None,
        output_parquet_path: Optional[str] = None,
        h3_resolution: Optional[int] = None,
        parent_resolutions: Optional[List[int]] = None,
        h0_index: Optional[int] = None,
        chunk_resolution: int = 0,
        chunk_index: Optional[int] = None,
        window_reads: str = "auto",
        h0_subset: Optional[List[int]] = None,
        h0_grid_path: str = "s3://public-grids/hex/h0-valid.parquet",
        value_column: str = "value",
        compression: str = "deflate",
        blocksize: int = 512,
        resampling: str = "nearest",
        hex_resampling: str = "mean",
        method: str = "exact-extract",
        nodata_value: Optional[Union[float, str, List[float]]] = None,
        target_crs: str = "EPSG:4326",
        target_extent: Optional[tuple] = None,
        target_resolution: Optional[float] = None,
        band: Optional[int] = None,
        local_cache_dir: Optional[str] = "/tmp/cng-raster-cache",
        read_credentials: Optional[Dict[str, str]] = None,
        write_credentials: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize the raster processor.

        Args:
            input_path: Path(s) to input raster file(s). A single string or a list of
                        tile URLs/paths (possibly in mixed CRS — they will be mosaicked
                        and reprojected to target_crs before processing).
            output_cog_path: Path for output COG file (optional)
            output_parquet_path: Base path for output parquet (e.g., s3://bucket/dataset/hex/)
            h3_resolution: Target H3 resolution (auto-detected if None)
            parent_resolutions: List of parent resolutions to include (e.g., [9, 8, 0])
            h0_index: Specific h0 cell index to process (0-121), or None for all
            chunk_resolution: H3 resolution of the unit of work. 0 (default) is
                one h0 base cell per invocation, the historical behaviour. A
                higher value splits each h0 into its res-N descendants, so peak
                memory — which tracks the largest chunk's cell count, not the
                raster's size — falls by roughly 7x per level (issue #173).
            chunk_index: Which chunk to process, indexing the ordered chunk list
                for chunk_resolution. At chunk_resolution 0 this is exactly
                h0_index; either may be given.
            window_reads: Whether a chunk reads only its own window of the
                source COG instead of localizing the whole file. "auto"
                (default) windows whenever chunk_resolution > 0, "always" and
                "never" force it. Full localization costs one whole-file copy
                per pod, which is tolerable across 122 h0 pods and ruinous
                across the thousands of pods sub-h0 chunking creates — the
                transfer scales with the fan-out, not with the data (issue
                #173, lever C).
            h0_subset: Restrict the chunk list to descendants of these h0 base
                cell indices, so a regional source never enumerates chunks it
                cannot overlap (issue #191, applied at chunk granularity).
            h0_grid_path: Path to h0 grid parquet file
            value_column: Name for the raster value column in parquet
            compression: Compression method for COG (deflate, lzw, zstd, etc.)
            blocksize: Block size for COG tiling
            resampling: Resampling method for COG creation (default: "nearest")
            hex_resampling: Reducer for aggregating source pixels into each
                H3 cell. With method="exact-extract" (default), one of:
                "sum" (counts/stocks like population), "mean" (intensities
                like NDVI), "mode" (categorical like land cover — single
                dominant class per cell), "fractions" (categorical area
                accounting — one LONG (value, frac) row per class present in
                each cell, with nodata kept as an explicit class so frac sums
                to <= 1 and area is SUM(frac * cell_area), issue #142),
                "max"/"min" (peak/extremum like species richness, where sum
                double-counts and mean averages away the hotspot). With
                method="warp-centroid", any GDAL resampleAlg is accepted
                ("average", "sum", "mode", "near", "bilinear", "cubic", ...).
                Default: "mean".
            method: Which raster→hex algorithm to use. "exact-extract"
                (default) is area-weighted, mass-conserving, one row per
                cell — recommended for stock rasters (population, carbon).
                "warp-centroid" is the older gdal.Warp→XYZ→centroid path:
                fast, low-memory, but emits one row per warped pixel
                (consumers GROUP BY h<res>). Mass-conserving only when the
                hex pitch is finer than the source pixel pitch — see issue
                #84 for the regime analysis.
            nodata_value: NoData value(s) to exclude from H3 conversion and to
                collapse in the COG warp. Accepts a single number, a list, or a
                comma-separated string ("-9999,-1111,32767") for categorical
                products with multiple fill codes (issue #108).
            target_crs: CRS for output (default: EPSG:4326); used when mosaicking
            target_extent: Clip extent (xmin, ymin, xmax, ymax) in target_crs; used when mosaicking
            target_resolution: Output pixel size in target_crs units; used when mosaicking
            band: Extract a single band from multi-band sources (1-indexed); used when mosaicking
            read_credentials: Dict with AWS credentials for reading
            write_credentials: Dict with AWS credentials for writing
        """
        _configure_proj()
        # If a list of tiles is provided, mosaic them into a temp COG first
        self._mosaic_tmpdir = None
        # A mosaic selects the band while it builds, so the single-band view
        # already exists by the time the selection below runs.
        band_applied_by_mosaic = False
        if isinstance(input_path, list):
            if len(input_path) == 1:
                input_path = input_path[0]
            else:
                band_applied_by_mosaic = band is not None
                import tempfile
                self._mosaic_tmpdir = tempfile.mkdtemp(prefix="raster_processor_")
                mosaic_path = os.path.join(self._mosaic_tmpdir, "mosaic.tif")
                print(f"Multiple input tiles detected ({len(input_path)}), mosaicking to temp COG...")
                create_mosaic_cog(
                    source_urls=input_path,
                    output_path=mosaic_path,
                    target_crs=target_crs,
                    target_extent=target_extent,
                    target_resolution=target_resolution,
                    band=band,
                    nodata=nodata_value,
                    resampling=resampling,
                    compression=compression,
                )
                input_path = mosaic_path

        # Default: localize remote inputs to local disk before processing.
        # exact_extract issues many small pixel-coverage queries against the
        # source raster; over /vsis3/ each one pays HTTP round-trip latency,
        # and on dense h0 cells (e.g. urban Asia at h9) workers spent ~95%
        # of wall time blocked on I/O — empirically ~12× slower than reading
        # from a local NVMe copy. Localization is a one-time per-pod cost
        # (~2 min for a 12 GB GHS-POP COG via rclone) that pays for itself
        # many times over. Pass local_cache_dir=None to opt out and stream
        # directly via /vsis3/ (useful for small rasters or non-cluster
        # environments without local disk headroom).
        # Checked here rather than at the warp so the build fails before it
        # localizes a multi-GB COG. Not silently downgraded to exact-extract:
        # the two methods do not produce the same thing (warp-centroid emits
        # one row per warped pixel, not one per cell, and is not
        # antimeridian-correct), so substituting one for the other would be a
        # wrong answer rather than a slower one.
        if method == "warp-centroid" and not gdal_supports_cutline_wkt():
            raise RuntimeError(
                f"method='warp-centroid' needs a GDAL whose Python bindings accept "
                f"WarpOptions(cutlineWKT=...); this GDAL is {gdal.__version__}, which "
                f"does not.\n"
                f"  Each h0 is warped clipped to its own boundary, so there is no "
                f"fallback within this method.\n"
                f"  Run in the project image (ghcr.io/boettiger-lab/datasets:latest), "
                f"or upgrade GDAL.\n"
                f"  method='exact-extract' works on this GDAL, but it is a different "
                f"aggregation — one area-weighted row per cell, rather than one row "
                f"per warped pixel — so switch to it deliberately, not as a drop-in."
            )

        if window_reads not in ("auto", "always", "never"):
            raise ValueError(
                f"window_reads must be 'auto', 'always' or 'never', got {window_reads!r}"
            )
        self.window_reads = window_reads
        # Windowing replaces the whole-file copy, so the two must not both run:
        # localizing first would pay exactly the cost windowing exists to avoid.
        # "auto" also requires the source to be remote — a window over a local
        # file transfers nothing and buys nothing, it just decodes and re-encodes
        # the region, so the honest default is to leave a local read alone.
        remote_source = isinstance(input_path, str) and (
            input_path.startswith("s3://")
            or input_path.startswith("http://")
            or input_path.startswith("https://")
        )
        self._windowing = (
            window_reads == "always"
            or (window_reads == "auto" and chunk_resolution > 0 and remote_source)
        )
        self._window_cache_dir = local_cache_dir
        # Enumerate only the subtrees that reach the source (issue #215).
        # Off via CNG_HEX_PRUNE_CELLS=0, which restores the full 7^n
        # enumeration for a like-for-like comparison.
        self._prune_cells = os.environ.get("CNG_HEX_PRUNE_CELLS", "1") != "0"

        if (local_cache_dir and not self._windowing
                and isinstance(input_path, str)
                and (input_path.startswith("s3://")
                     or input_path.startswith("http://")
                     or input_path.startswith("https://"))):
            input_path = _localize_input(input_path, local_cache_dir)

        # Use /vsis3/ so reads honor AWS_S3_ENDPOINT — inside the cluster this
        # routes to the internal Ceph endpoint (e.g. rook-ceph-rgw-nautiluss3.rook),
        # not the external s3-west.nrp-nautilus.io load balancer.
        self.input_path = _ensure_vsi_path(input_path, use_public_endpoint=False)

        # Band selection, before anything else reads the raster (issue #214).
        # The hex path used to ignore --band entirely and let exactextract fall
        # back to the first band, which is a silent wrong answer rather than a
        # failure — so an unselected multi-band source is refused outright, and
        # a selected one is narrowed here so no later step can pick differently.
        self.band = band
        if band is not None and not band_applied_by_mosaic:
            total_bands = band_count(self.input_path)
            self.input_path = band_subset_vrt(self.input_path, band)
            # Detection below reads the local name, so point it at the same view.
            input_path = self.input_path
            print(f"✓ Using band {band} of {total_bands}")
        if output_parquet_path is not None:
            assert_band_is_unambiguous(self.input_path, band)

        # Warn if input is in a projected CRS — reprojection to EPSG:4326 will happen
        # internally, but a projected input can cause silent failures if PROJ is misconfigured.
        _ds = gdal.Open(self.input_path)
        if _ds is not None:
            _srs = osr.SpatialReference(wkt=_ds.GetProjection())
            _ds = None
            if not _srs.IsGeographic():
                try:
                    _srs.AutoIdentifyEPSG()
                except RuntimeError:
                    pass  # no EPSG authority code (e.g. ESRI:102003); proceed with WKT/PROJ definition
                _epsg = _srs.GetAuthorityCode(None)
                _crs_name = f"EPSG:{_epsg}" if _epsg else _srs.GetName() or "unknown projected CRS"
                print(
                    f"⚠ Input raster is in a projected CRS ({_crs_name}), not WGS84/EPSG:4326.\n"
                    f"  It will be reprojected to EPSG:4326 during processing.\n"
                    f"  For best results, reproject first: "
                    f"gdalwarp -t_srs EPSG:4326 input.tif output-wgs84.tif"
                )

        self.output_cog_path = output_cog_path
        self.output_parquet_path = output_parquet_path
        self.h0_index = h0_index
        if chunk_resolution < 0:
            raise ValueError(
                f"chunk_resolution must be >= 0, got {chunk_resolution}"
            )
        self.chunk_resolution = chunk_resolution
        # h0_index and chunk_index are the same selector at chunk_resolution 0;
        # accepting both keeps every existing caller and manifest working.
        if chunk_index is None:
            chunk_index = h0_index
        elif h0_index is not None and chunk_index != h0_index:
            raise ValueError(
                f"chunk_index ({chunk_index}) and h0_index ({h0_index}) both given "
                "and disagree; pass only one"
            )
        self.chunk_index = chunk_index
        self.h0_subset = sorted({int(h) for h in h0_subset}) if h0_subset else None
        self._chunk_cells_cache = None
        self.h0_grid_path = h0_grid_path
        self.value_column = value_column
        self.compression = compression
        self.blocksize = blocksize
        self.resampling = resampling
        if method not in VALID_METHODS:
            raise ValueError(
                f"method must be one of {list(VALID_METHODS)}, got {method!r}."
            )
        self.method = method

        # hex_resampling vocabulary depends on the method.
        # exact-extract: only the exact-extract reducers (sum/mean/mode/max/min).
        # warp-centroid: any GDAL resampleAlg is forwarded to gdal.Warp.
        if method == "exact-extract":
            if hex_resampling not in VALID_HEX_REDUCERS:
                raise ValueError(
                    f"With method='exact-extract', hex_resampling must be one of "
                    f"{list(VALID_HEX_REDUCERS)}; got {hex_resampling!r}. "
                    f"Use method='warp-centroid' for the older GDAL-Warp "
                    f"resampling vocabulary (average, near, bilinear, cubic, ...)."
                )
        else:  # warp-centroid
            if hex_resampling not in VALID_WARP_RESAMPLERS:
                raise ValueError(
                    f"With method='warp-centroid', hex_resampling must be a "
                    f"GDAL resampleAlg value (e.g. {list(VALID_WARP_RESAMPLERS[:6])}); "
                    f"got {hex_resampling!r}."
                )
        self.hex_resampling = hex_resampling
        self.read_credentials = read_credentials
        self.write_credentials = write_credentials

        # Auto-detect NoData value if not specified. Categorical products carry
        # several fill codes, so nodata is tracked as a list (issue #108); the
        # single-value paths (VRT build, detection) use self.nodata_value, the
        # first entry, for backwards compatibility.
        parsed_nodata = _parse_nodata_values(nodata_value)
        if not parsed_nodata:
            detected_nodata = detect_nodata_value(input_path, verbose=True)
            if detected_nodata is not None:
                self.nodata_values = [detected_nodata]
            else:
                self.nodata_values = []
                print("ℹ No NoData value specified or detected - all values will be included")
        else:
            self.nodata_values = parsed_nodata
            print(f"✓ Using user-specified NoData value(s): {parsed_nodata}")
        self.nodata_value = self.nodata_values[0] if self.nodata_values else None

        # Handle H3 resolution with informative messages
        detected_resolution = detect_optimal_h3_resolution(input_path, verbose=False)

        if h3_resolution is None:
            # Use auto-detected resolution
            self.h3_resolution = detected_resolution
            print(f"✓ Auto-detected H3 resolution: h{detected_resolution}")
        else:
            # User specified a resolution - compare with detection
            self.h3_resolution = h3_resolution

            if h3_resolution != detected_resolution:
                if h3_resolution < detected_resolution:
                    print(f"ℹ Using coarser resolution h{h3_resolution} (user specified) instead of detected h{detected_resolution}")
                    print("  Note: Coarser resolution will aggregate more pixels per H3 cell")
                else:
                    print(f"ℹ Using finer resolution h{h3_resolution} (user specified) instead of detected h{detected_resolution}")
                    print("  Note: Finer resolution will create more H3 cells and larger output files")
            else:
                print(f"✓ Using h{h3_resolution} (matches auto-detected resolution)")

        self.parent_resolutions = parent_resolutions or []

        # A build with no path to h8 — target too coarse, or h8 missing from the
        # parents — is silently non-joinable unless we say so (issue #182).
        join_warning = h3_resolution_join_warning(
            self.h3_resolution,
            user_specified=h3_resolution is not None,
            parent_resolutions=self.parent_resolutions,
        )
        if join_warning:
            print(join_warning)

        # Set up DuckDB connection
        self.con = self._setup_duckdb()

        # Pre-compute source raster bounds in EPSG:4326 for fast h0 intersection checks
        self._src_bounds_4326 = self._compute_src_bounds_4326()

    def _setup_duckdb(self) -> duckdb.DuckDBPyConnection:
        """Set up DuckDB connection with extensions."""
        con = duckdb.connect()

        # Install and load extensions
        con.execute("INSTALL spatial")
        con.execute("LOAD spatial")
        con.execute("INSTALL h3 FROM community")
        con.execute("LOAD h3")

        # Configure HTTP settings
        con.execute("SET http_retries=20")
        con.execute("SET http_retry_wait_ms=5000")
        con.execute("SET temp_directory='/tmp'")

        # Bound DuckDB, and let it spill rather than compete with the workers.
        #
        # Unset, DuckDB sizes its buffer manager from the *host's* RAM — 80% of
        # it — which inside a pod is neither the pod's limit nor anything the
        # manifest asked for. The hex step's final COPY scans every part the
        # workers wrote, so without a limit that scan buffers in proportion to
        # the cells in the chunk and the process grows until the cgroup kills
        # it. With a limit it spills to temp_directory instead, which is what
        # makes the step's memory a function of the limit rather than of the
        # data (issue #173). The merge and repartition steps have always done
        # this; the hex step never did.
        requested_limit = os.environ.get("DUCKDB_MEMORY_LIMIT")
        if requested_limit:
            # Normalised because the value reaching here has usually passed
            # through a Kubernetes manifest, and DuckDB rejects the k8s
            # spelling of it (issue #217): "16Gi" is a parser error.
            effective = to_duckdb_memory_limit(requested_limit)
            suffix = "" if effective == requested_limit else f" (from {requested_limit})"
            print(f"  Setting DuckDB memory_limit={effective}{suffix}")
            con.execute(f"SET memory_limit='{effective}'")

        # Configure S3 credentials
        configure_s3_credentials(con)

        return con

    def _compute_src_bounds_4326(self) -> tuple:
        """Return (xmin, ymin, xmax, ymax) of the source raster in EPSG:4326."""
        ds = gdal.Open(self.input_path)
        if ds is None:
            raise ValueError(f"Could not open raster to compute bounds: {self.input_path}")
        gt = ds.GetGeoTransform()
        xmin = gt[0]
        xmax = gt[0] + gt[1] * ds.RasterXSize
        ymax = gt[3]
        ymin = gt[3] + gt[5] * ds.RasterYSize
        src_srs = osr.SpatialReference()
        src_srs.ImportFromWkt(ds.GetProjection())
        src_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

        if src_srs.IsGeographic():
            ds = None
            return (min(xmin, xmax), min(ymin, ymax), max(xmin, xmax), max(ymin, ymax))

        # Projected source: compute the EPSG:4326 extent the way gdalwarp does,
        # via AutoCreateWarpedVRT. Transforming only the four rectangular-extent
        # corners point-by-point is unsafe for a global equal-area projection
        # such as World Mollweide (ESRI:54009): the rectangle's corners fall in
        # the projection's undefined oval corners, so the transform raises
        # "Point outside of projection domain" and crashes __init__ before any
        # work is done (issue #151). Densifying the rectangle edges doesn't help
        # either — every edge point of the bounding rectangle is still in the
        # undefined region, which collapses the longitude span. AutoCreateWarpedVRT
        # samples the raster the way the warp machinery itself does (this is the
        # file gdalwarp -t_srs EPSG:4326 handles fine) and yields a correct,
        # domain-safe extent.
        tgt_srs = osr.SpatialReference()
        tgt_srs.ImportFromEPSG(4326)
        tgt_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

        bounds = None
        try:
            vrt = gdal.AutoCreateWarpedVRT(ds, ds.GetProjection(), tgt_srs.ExportToWkt())
            if vrt is not None:
                vgt = vrt.GetGeoTransform()
                vxmin = vgt[0]
                vxmax = vgt[0] + vgt[1] * vrt.RasterXSize
                vymax = vgt[3]
                vymin = vgt[3] + vgt[5] * vrt.RasterYSize
                vrt = None
                if all(math.isfinite(v) for v in (vxmin, vymin, vxmax, vymax)):
                    bounds = (min(vxmin, vxmax), min(vymin, vymax),
                              max(vxmin, vxmax), max(vymin, vymax))
        except RuntimeError:
            bounds = None
        finally:
            ds = None

        if bounds is None:
            # Last-resort safe over-approximation: the whole globe. These bounds
            # are only used to clip/skip h0 regions during hex aggregation, so an
            # over-approximation is safe (it just skips fewer regions) while an
            # under-approximation would silently drop real data. Better than the
            # historic crash.
            print("  ⚠ Could not compute reprojected bounds; assuming global extent.")
            bounds = (-180.0, -90.0, 180.0, 90.0)
        return bounds

    def create_cog(
        self,
        output_path: Optional[str] = None,
        overviews: bool = True,
        overview_resampling: Optional[str] = None,
    ) -> str:
        """
        Create a Cloud-Optimized GeoTIFF from input raster.

        Optimized for cloud rendering in services like titiler with:
        - Internal tiling
        - Overview pyramids
        - Optimized compression
        - EPSG:4326 reprojection

        Args:
            output_path: Path for output COG (uses self.output_cog_path if None)
            overviews: Whether to create overview pyramids
            overview_resampling: Resampling method for overviews. Defaults to
                "mode" when hex_resampling is categorical ("mode"/"fractions" —
                averaging class codes corrupts the zoomed-out COG, issue #108)
                and "average" otherwise.

        Returns:
            Path to created COG file
        """
        if output_path is None:
            output_path = self.output_cog_path

        if output_path is None:
            raise ValueError("output_path or output_cog_path must be specified")

        if overview_resampling is None:
            # Categorical rasters (hex_resampling "mode"/"fractions") must not
            # average their class codes in overviews — use mode (issue #108).
            categorical = self.hex_resampling in ("mode", "fractions")
            overview_resampling = "mode" if categorical else "average"

        print(f"Creating COG: {output_path}")
        print(f"  Input: {self.input_path}")

        cog_creation_opts = [
            f'COMPRESS={self.compression.upper()}',
            f'BLOCKSIZE={self.blocksize}',
            'BIGTIFF=IF_SAFER',
            'NUM_THREADS=ALL_CPUS',
        ]

        # Reproject to EPSG:4326 if needed
        ds = gdal.Open(self.input_path)
        if ds is None:
            raise ValueError(f"Could not open input raster: {self.input_path}")

        srs = osr.SpatialReference(wkt=ds.GetProjection())
        needs_reprojection = not srs.IsGeographic()
        ds = None

        # NoData handling (issue #108): a band carries only one nodata value, so
        # the primary (first) value is declared on the warp/translate and the
        # remaining fill codes are remapped to it afterwards. Previously
        # create_cog applied no nodata at all, so secondary fill codes (LANDFIRE
        # -9999/-1111) survived as "valid" classes into the hex step.
        nodata_values = self.nodata_values
        primary_nodata = nodata_values[0] if nodata_values else None

        workdir = tempfile.mkdtemp(prefix="create_cog_")
        try:
            tmp_tif = os.path.join(workdir, "intermediate.tif")

            if needs_reprojection:
                print("  Reprojecting to EPSG:4326...")
                warp_kwargs = dict(
                    dstSRS='EPSG:4326',
                    format='GTiff',
                    creationOptions=['COMPRESS=NONE', 'BIGTIFF=IF_SAFER'],
                    resampleAlg=self.resampling,
                    multithread=True,
                )
                if primary_nodata is not None:
                    warp_kwargs["srcNodata"] = _fmt_gdal(primary_nodata)
                    warp_kwargs["dstNodata"] = primary_nodata
                result = gdal.Warp(tmp_tif, self.input_path,
                                   options=gdal.WarpOptions(**warp_kwargs))
            else:
                translate_kwargs = dict(
                    format='GTiff',
                    creationOptions=['COMPRESS=NONE', 'BIGTIFF=IF_SAFER'],
                )
                if primary_nodata is not None:
                    translate_kwargs["noData"] = primary_nodata
                result = gdal.Translate(tmp_tif, self.input_path, **translate_kwargs)

            if result is None:
                raise RuntimeError(f"Failed to create intermediate GTiff: {gdal.GetLastErrorMsg()}")
            result = None

            # Collapse any secondary fill codes to the primary nodata before
            # building overviews, so the overviews and the COG carry a single,
            # consistent nodata (issue #108).
            if len(nodata_values) > 1:
                print(f"  Collapsing fill codes {nodata_values} → {primary_nodata}...")
                _collapse_fill_values(tmp_tif, nodata_values, primary_nodata)

            if overviews:
                print("  Building overviews...")
                tmp_ds = gdal.Open(tmp_tif, gdal.GA_Update)
                tmp_ds.BuildOverviews(overview_resampling.upper(), [2, 4, 8, 16, 32, 64])
                tmp_ds = None
                cog_creation_opts.append('COPY_SRC_OVERVIEWS=YES')
                cog_creation_opts.append(f'OVERVIEW_RESAMPLING={overview_resampling.upper()}')

            print("  Writing COG...")
            vsi_output = _ensure_vsi_path(output_path)
            # COG driver requires random-write access; /vsis3/ needs this config option.
            if vsi_output.startswith("/vsis3/"):
                gdal.SetConfigOption("CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE", "YES")
            result = gdal.Translate(
                vsi_output,
                tmp_tif,
                format='COG',
                creationOptions=cog_creation_opts,
            )
        finally:
            import shutil
            shutil.rmtree(workdir, ignore_errors=True)

        if result is None:
            raise RuntimeError(f"Failed to create COG: {gdal.GetLastErrorMsg()}")

        result = None  # Close dataset

        print(f"  ✓ COG created: {output_path}")
        return output_path

    def _native_cells_for_h0(self, h0_cell: int):
        """Return the native-resolution H3 cell ids of one h0 partition as a
        uint64 numpy array.

        Ids only: the boundary WKT each worker needs is derived from the id
        inside the worker (`_boundary_wkt_for`). Fetching boundaries here made
        them ~96% of a list that is materialised in full before any work
        starts, which was the dominant term in this process's peak RSS —
        183 bytes per cell against 8 for the id alone, and ~485 vs ~121 bytes
        of peak RSS per cell once DuckDB's own materialisation is counted
        (issue #173). A numpy array is also what keeps the chunking below from
        building one Python object per cell.

        Cells come from h3_cell_to_children(h0, res) — the exact H3 hierarchy
        traversal. Every native cell has exactly one res-0 parent, so the 122
        per-h0 cell sets are disjoint and cover the globe exactly: no overlap
        (issue #89's cross-partition boundary duplication) and no gaps. This
        replaces a polygon polyfill (h3_polygon_wkt_to_cells on the stored h0
        geometry), which selected strays from neighbouring h0s, missed some
        true children, and — for the antimeridian h0s whose stored polygon
        spans -178..+177 in planar lat/lon — collapsed to zero cells (#88).

        The child count is ~7^(h3_resolution) - comfortably millions at h9 but
        tens-of-millions at h11; the caller chunks it across worker processes.
        """
        h3_col = f"h{self.h3_resolution}"
        import numpy as np

        sql, pruned_from = self._enumeration_sql(h0_cell, h3_col)
        cells = self.con.execute(sql).fetchnumpy()[h3_col]
        if pruned_from is not None:
            # The number that explains this pod's peak memory and runtime, at
            # the point it is decided rather than afterwards (issue #215).
            share = 100.0 * len(cells) / pruned_from if pruned_from else 0.0
            print(f"  ✓ {len(cells):,} of {pruned_from:,} cells reach the source "
                  f"({share:.1f}% of the chunk)")
        # fetchnumpy hands UBIGINT back as int64. Bit 63 of an H3 index is
        # reserved and always 0, so every cell id fits in an int64 and the two
        # views share a bit pattern — .view() relabels in place rather than
        # copying an array that is 2.3 GB at res 10.
        return cells.view(np.uint64)

    def _enumeration_sql(self, chunk_cell: int, h3_col: str):
        """`(sql, pruned_from)` — the query that lists one chunk's native cells.

        Either every descendant of the chunk cell, or — when the source covers
        only part of it — the descendants of the subtrees that reach the source
        (issue #215). Both forms return the same column, so the caller and the
        workers below it are unchanged. `pruned_from` is the chunk's full child
        count when the prune ran, and None when it did not, so the caller can
        report what the restriction bought.
        """
        full = f"""
            WITH native_cells AS (
                SELECT UNNEST(
                    h3_cell_to_children({chunk_cell}, {self.h3_resolution})
                ) AS cell
            )
            SELECT cell AS {h3_col}
            FROM native_cells
        """
        if not self._prune_cells:
            return full, None
        sxmin, symin, sxmax, symax = self._src_bounds_4326
        if (sxmin <= -180.0 and sxmax >= 180.0
                and symin <= -90.0 and symax >= 90.0):
            # A global source (or one whose bounds could not be computed, which
            # falls back to global) has nothing to prune against.
            return full, None

        start = self.con.execute(
            f"SELECT h3_get_resolution({int(chunk_cell)}::UBIGINT)"
        ).fetchone()[0]
        if int(start) >= self.h3_resolution:
            # The chunk cell is already at (or below) the native resolution, so
            # there is no hierarchy to descend and h3_cell_to_children returns
            # the cell itself. Pruning here would mean deciding the chunk's fate
            # twice, on the same geometry the caller has already tested.
            return full, None

        interior, leaves = self._classify_descendants(chunk_cell, int(start))
        total = self.con.execute(
            f"SELECT h3_cell_to_children_size({int(chunk_cell)}::UBIGINT, "
            f"{self.h3_resolution})"
        ).fetchone()[0]
        if not interior and not leaves:
            return f"SELECT NULL::UBIGINT AS {h3_col} WHERE false", int(total)
        parts = []
        if interior:
            values = ",".join(f"({int(c)}::UBIGINT)" for c in interior)
            parts.append(
                f"SELECT UNNEST(h3_cell_to_children(cell, {self.h3_resolution})) "
                f"AS {h3_col} FROM (VALUES {values}) t(cell)"
            )
        if leaves:
            values = ",".join(f"({int(c)}::UBIGINT)" for c in leaves)
            parts.append(f"SELECT cell AS {h3_col} FROM (VALUES {values}) t(cell)")
        return " UNION ALL ".join(parts), int(total)

    def _classify_descendants(self, chunk_cell: int, start: int):
        """Descend the H3 hierarchy, keeping only what can reach the source.

        Returns `(interior, leaves)`: cells whose every descendant is kept, and
        individual native-resolution cells. The caller expands `interior` with
        `h3_cell_to_children`, so nothing here is proportional to the number of
        cells kept — only to the number examined.

        At each level a cell is one of three things:

        - **outside** the source, widened by the prune margin — dropped, along
          with its whole subtree;
        - **inside** the source outright — kept wholesale without descending,
          which is what keeps a raster that fills its chunk from costing more
          than it does today;
        - **straddling** the source's edge — descended one level further.

        So the work is proportional to the source's *perimeter* rather than its
        area. The footprint tested is the one `_cell_footprint` defines — a
        planar envelope is exactly that for any cell not straddling +/-180, and
        the ones that do straddle are read from their rings — so the prune and
        the overlap test cannot disagree about where a cell is.

        The margin is what makes dropping safe. H3's hierarchy is only
        approximately containing — a descendant can protrude past its parent's
        boundary — and the protrusions compound down the levels, bounded by
        roughly `_H3_PROTRUSION_MARGIN / (1 - 1/sqrt(7))` ~ 1.6x one level's.
        `_H3_PRUNE_MARGIN_FACTOR` carries 2x, comfortably outside that, and the
        gate is measured rather than argued: against exhaustive enumeration of
        all 5,764,801 res-8 children of an h0, this drops none of the 29,055
        cells that genuinely touch the raster, and none of the 620 straddling
        the antimeridian (issue #215).

        Containment needs no margin, only the opposite bias: taking a subtree
        wholesale can over-include, and an extra cell yields no covered pixels
        and is dropped by the aggregation. Under-including cannot be recovered.
        """
        sxmin, symin, sxmax, symax = self._src_bounds_4326
        frontier = [int(chunk_cell)]
        interior, leaves = [], []
        for level in range(start + 1, self.h3_resolution + 1):
            if not frontier:
                break
            values = ",".join(f"({c}::UBIGINT)" for c in frontier)
            # Envelopes in SQL rather than shapely: at fine resolutions this
            # loop sees tens of thousands of cells, and parsing a WKT ring per
            # cell to recover a bounding box it already has costs more than the
            # prune saves. A planar envelope is the whole footprint for every
            # cell that does not straddle +/-180 — and the ones that do are
            # re-derived from their rings below.
            rows = self.con.execute(f"""
                WITH f AS (SELECT * FROM (VALUES {values}) t(cell)),
                     kids AS (SELECT UNNEST(h3_cell_to_children(cell, {level})) AS cell FROM f),
                     env AS (SELECT cell,
                                    ST_Envelope(ST_GeomFromText(h3_cell_to_boundary_wkt(cell))) AS e
                             FROM kids)
                SELECT cell, ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e) FROM env
            """).fetchall()
            straddlers = [r[0] for r in rows if r[3] - r[1] > 180]
            rings = {}
            if straddlers:
                sv = ",".join(f"({int(c)}::UBIGINT)" for c in straddlers)
                rings = dict(self.con.execute(
                    f"SELECT cell, h3_cell_to_boundary_wkt(cell) "
                    f"FROM (VALUES {sv}) t(cell)"
                ).fetchall())
            frontier = []
            for cell, xmin, ymin, xmax, ymax in rows:
                if cell in rings:
                    ymin, ymax, lon_intervals = _cell_footprint(rings[cell])
                else:
                    lon_intervals = [(xmin, xmax)]
                extent = ymax - ymin
                margin = _H3_PRUNE_MARGIN_FACTOR * _H3_PROTRUSION_MARGIN * extent
                wymin, wymax, wide = _widen_footprint(ymin, ymax, lon_intervals, margin)
                if symax < wymin or symin > wymax:
                    continue
                if not any(not (sxmax < lo or sxmin > hi) for lo, hi in wide):
                    continue
                if level == self.h3_resolution:
                    leaves.append(int(cell))
                elif (len(lon_intervals) == 1
                      and symin <= ymin and symax >= ymax
                      and sxmin <= lon_intervals[0][0] and sxmax >= lon_intervals[0][1]):
                    # Wholly inside the source: every descendant is kept. A cell
                    # straddling +/-180 is never claimed here — its two intervals
                    # would each have to be contained, and descending it instead
                    # costs work rather than data.
                    interior.append(int(cell))
                else:
                    frontier.append(int(cell))
        return interior, leaves

    def _h0_overlaps_raster(self, h0_geom_wkt: str, margin_deg: float = 0.0) -> bool:
        """Whether the source raster's extent overlaps an h0 cell's true
        footprint, optionally widened by a margin.

        The margin exists because H3's hierarchy is only *approximately*
        containing: a child cell can protrude slightly beyond its parent's
        boundary polygon. Pruning a sub-chunk on its own boundary therefore
        drops native cells that really do overlap the raster — silently, with
        a clean exit and a plausible output (issue #173). Callers processing a
        sub-chunk pass a margin that generously bounds the protrusion. The
        asymmetry is deliberate: a false positive costs one pod that exits in
        seconds, a false negative costs data with nothing to show for it.

        The stored h0 polygon is in planar lat/lon, so antimeridian h0s
        (vertices on both sides of +/-180) have a bounding box ~360 deg wide
        that both fails to prune anywhere and wrongly excludes the +/-180 strip
        where their data lives. We unwrap the longitudes (negatives +360); a
        span > 180 deg means the cell straddles the antimeridian, so its
        longitude footprint is two intervals on [-180, 180]. Latitude is never
        wrapped, so the polygon's lat bounds are used directly.
        """
        miny, maxy, lon_intervals = _cell_footprint(h0_geom_wkt, margin_deg)
        sxmin, symin, sxmax, symax = self._src_bounds_4326  # (xmin,ymin,xmax,ymax)
        if symax < miny or symin > maxy:  # no latitude overlap
            return False
        return any(not (sxmax < lo or sxmin > hi) for lo, hi in lon_intervals)

    def _collapsed_aggregation_input(self, source_path: Optional[str] = None) -> str:
        """Local raster with every fill code collapsed to the primary nodata.

        Built once and reused across all h0 regions. Needed only when more than
        one fill code is requested (issue #108): exactextract honors a single
        band nodata, so the extra codes must be physically remapped first. The
        raster is staged to a local GTiff (GA_Update needs a writable file, not
        a /vsis3/ object) and collapsed block-wise so a continent-scale source
        never has to fit in memory.

        An integer source takes the free path: the remap is expressed as a VRT
        lookup table and no pixels are written at all. Only a float source —
        where a lookup table would interpolate rather than substitute — falls
        back to a materialised copy, which is compressed, because the
        uncompressed one cost `grid pixels x bytes per pixel` on every pod's
        ephemeral disk regardless of the chunk that pod was working (issue
        #209).

        In the standard raster-workflow the COG step already collapses the fill
        codes, so the hex job is handed only the primary value and never reaches
        this path; it covers running the hex step directly on a multi-fill
        source, which is what a COG source does, since it needs no preprocess
        step to convert.
        """
        source_path = source_path or self.input_path
        # Keyed by source: with windowed reads (issue #173 lever C) each chunk
        # collapses its own window, so a single cached copy would hand one
        # chunk another chunk's pixels.
        cache = getattr(self, "_collapsed_inputs", None)
        if cache is None:
            cache = self._collapsed_inputs = {}
        if source_path in cache:
            return cache[source_path]
        primary = self.nodata_values[0]
        stem = os.path.join(
            tempfile.gettempdir(),
            f"cng_collapsed_{abs(hash(source_path)) % (10 ** 12)}",
        )
        print(f"  Collapsing fill codes {self.nodata_values} → {primary} for hex aggregation...")

        as_vrt = _fill_collapse_vrt(
            source_path, self.nodata_values, primary, stem + ".vrt"
        )
        if as_vrt is not None:
            print(f"  ✓ Collapsed as a VRT lookup table, no raster written: {as_vrt}")
            cache[source_path] = as_vrt
            return as_vrt

        collapsed = stem + ".tif"
        print("  ℹ No lookup table for this source, so the collapse is "
              "materialised; this writes the whole grid to local disk")
        result = gdal.Translate(
            collapsed,
            source_path,
            format="GTiff",
            creationOptions=["BIGTIFF=IF_SAFER", "NUM_THREADS=ALL_CPUS",
                             "TILED=YES", "COMPRESS=ZSTD",
                             f"PREDICTOR={_compression_predictor(source_path)}"],
        )
        if result is None:
            raise RuntimeError(
                f"Failed to stage raster for fill-code collapse: {gdal.GetLastErrorMsg()}"
            )
        result = None
        _collapse_fill_values(collapsed, self.nodata_values, primary)
        cache[source_path] = collapsed
        return collapsed

    def _hex_aggregate_h0(self, chunk_cell: int, h0_cell: Optional[int] = None,
                          source_path: Optional[str] = None) -> Optional[str]:
        """Area-weighted aggregation of source raster into native H3 cells
        inside one h0 partition.

        Uses exactextract for fractional-pixel coverage so SUM/mean/mode
        are mass-conserving regardless of source-pixel vs hex-pitch ratio.
        Chunks the cell list and runs exact_extract in parallel across
        worker processes; each worker reopens the raster (private
        /vsicurl/ handle) and processes its chunk independently.

        Tunables (env vars):
          CNG_HEX_CHUNK_SIZE — cells per worker call (default 100000)
          CNG_HEX_WORKERS    — process pool size (default = cgroup CPU quota)

        Returns the output parquet path, or None if no cells produced values.
        """
        import rasterio
        from concurrent.futures import ProcessPoolExecutor

        h3_col = f"h{self.h3_resolution}"
        # chunk_cell is the unit of work and defines which native cells are
        # enumerated; h0_cell is only the partition the output lands in. They
        # differ whenever chunk_resolution > 0 (issue #173).
        if h0_cell is None:
            h0_cell = chunk_cell
        # source_path is the windowed copy when lever C is active; everything
        # below reads it in place of the full raster.
        source_path = source_path or self.input_path
        cells_arr = self._native_cells_for_h0(chunk_cell)

        if len(cells_arr) == 0:
            print(f"  ℹ chunk {chunk_cell}: no h{self.h3_resolution} cells")
            return None

        # exactextract excludes pixels equal to the raster's single declared
        # nodata. A lone requested value just overrides the band nodata via a
        # cheap VRT; multiple fill codes (issue #108) cannot be expressed as one
        # band nodata, so they are physically remapped to the primary in a
        # collapsed copy built once and reused across h0 regions.
        nodata_values = self.nodata_values
        is_fractions = self.hex_resampling == "fractions"
        # For "fractions" the nodata code is KEPT as an explicit class (#142) so
        # frac is the class's share of the cell *including* nodata, sums to <= 1,
        # and the nodata/unclassified share is recoverable (rather than silently
        # re-inflating the real classes the way valid-coverage normalization
        # would). Cells with at least one nodata code that we still want labelled
        # explicitly are filtered below; the codes that mark "nodata" downstream:
        nodata_codes = [nodata_values[0]] if (is_fractions and nodata_values) else []
        with rasterio.open(source_path) as rast:
            src_nodata = rast.nodata

        vrt_path = None
        try:
            if not nodata_values:
                rast_arg = source_path
            elif is_fractions:
                # Collapse multi-fill to the primary first (#108), then CLEAR the
                # band nodata so exactextract returns it as a normal class. The
                # collapse step sets band nodata = primary, so a no-nodata VRT is
                # built over whichever base we use.
                base = (
                    self._collapsed_aggregation_input(source_path)
                    if len(nodata_values) > 1 else source_path
                )
                vrt_path = f"/tmp/raster_{chunk_cell}_keepnodata.vrt"
                gdal.Translate(vrt_path, base, format="VRT", noData="none")
                rast_arg = vrt_path
            elif len(nodata_values) == 1:
                if src_nodata != nodata_values[0]:
                    vrt_path = f"/tmp/raster_{chunk_cell}_nodata.vrt"
                    gdal.Translate(
                        vrt_path,
                        source_path,
                        format="VRT",
                        noData=nodata_values[0],
                    )
                    rast_arg = vrt_path
                else:
                    rast_arg = source_path
            else:
                rast_arg = self._collapsed_aggregation_input(source_path)

            chunk_size = int(os.environ.get("CNG_HEX_CHUNK_SIZE", "100000"))
            n_workers = int(os.environ.get("CNG_HEX_WORKERS", str(_default_hex_workers())))
            n_workers = max(1, n_workers)

            # Chunks are views into the uint64 id array, so nothing here is
            # proportional to the cell count beyond the array itself: no
            # per-cell Python objects, no boundary strings. Each worker turns
            # its own chunk of ids into (id, wkt) pairs and reconstructs the
            # geometries — shapely objects still must not cross the process
            # boundary, since deserializing them is slow (issue #173).
            chunks = [cells_arr[i:i + chunk_size]
                      for i in range(0, len(cells_arr), chunk_size)]

            parts_dir = os.path.join(
                tempfile.gettempdir(), f"cng_hex_parts_{chunk_cell}_{os.getpid()}"
            )
            args_iter = [(rast_arg, self.hex_resampling, c, parts_dir, i)
                         for i, c in enumerate(chunks)]
            print(
                f"  exact_extract: {sum(len(c) for c in chunks)} cells in "
                f"{len(chunks)} chunks (size {chunk_size}) × {n_workers} workers"
            )

            # Workers return paths, not rows. The parent therefore holds
            # nothing proportional to the cell count, and DuckDB reads the
            # parts as one scan — streaming, and bounded by its own memory
            # limit rather than by what fits in this process (issue #173).
            # Only as many workers as there are chunks ever run at once, and
            # the model below is in terms of what actually ran.
            workers_used = min(n_workers, len(chunks))
            if n_workers == 1 or len(chunks) == 1:
                parts = [_exact_extract_chunk(a) for a in args_iter]
            else:
                with ProcessPoolExecutor(max_workers=n_workers) as ex:
                    parts = list(ex.map(_exact_extract_chunk, args_iter))

            parts = [p for p in parts if p]
            if not parts:
                print(f"  ℹ chunk {chunk_cell}: no cells produced values (all chunks empty)")
                return None

            output_path = self._write_partition(
                parts, parts_dir, chunk_cell, h0_cell, h3_col,
                is_fractions, nodata_codes,
            )
        finally:
            if vrt_path is not None and os.path.exists(vrt_path):
                os.remove(vrt_path)
            # Always, not only on the way out of the COPY: a chunk whose
            # workers all found nothing returns before writing anything, and
            # an exception anywhere above leaves the parts behind too. A pod
            # processes many chunks, so a leak here accumulates.
            if parts_dir is not None:
                shutil.rmtree(parts_dir, ignore_errors=True)

        if output_path is None:
            print(f"  ℹ chunk {chunk_cell}: no cells produced values (all nodata)")
            return None

        # Fail fast if the h3 extension emitted signed BIGINT parents (issue #102):
        # the native cell column is UBIGINT, but h3_cell_to_parent's return type
        # depends on the (unpinned) extension version. Assert per-partition since
        # each h0 region is written by an independent job.
        assert_h3_columns_unsigned(
            lambda sql: self.con.execute(sql).fetchall(), output_path
        )

        written = self.con.execute(
            f"SELECT count(*) FROM read_parquet('{output_path}')"
        ).fetchone()[0]
        unit = "class rows" if is_fractions else "cells"
        print(f"  ✓ Wrote: {output_path} ({written} {unit})")
        _report_memory_model(len(cells_arr), workers_used)
        return output_path

    def _write_partition(self, parts, parts_dir, chunk_cell, h0_cell, h3_col,
                         is_fractions, nodata_codes):
        """One DuckDB statement from the workers' parts to the partition.

        Returns the path written, or None when every row was filtered out.
        Nothing here is materialised in this process: the parts are a parquet
        scan and the output is a COPY, so the parent's memory is a function of
        DuckDB's limit rather than of the chunk's cell count (issue #173).
        """
        parent_exprs = []
        for parent_res in sorted(self.parent_resolutions):
            if parent_res < self.h3_resolution:
                col_name = f"h{parent_res}"
                parent_exprs.append(
                    f"h3_cell_to_parent({h3_col}, {parent_res}) AS {col_name}"
                )
        parent_sql = ", " + ", ".join(parent_exprs) if parent_exprs else ""

        output_path = self._chunk_output_path(chunk_cell, h0_cell)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # The parts, renamed into the caller's vocabulary. Everything below is
        # one DuckDB statement over a parquet scan, so no result set is
        # materialised in this process at any point.
        #
        # Read by glob, but checked against the paths the workers actually
        # reported: a scan that silently picks up a different set of files than
        # the run produced is the failure this project has paid for most often
        # (issue #208), and here it would be invisible — the output would be a
        # valid parquet of the wrong size.
        #
        parts_glob = os.path.join(parts_dir, "part-*.parquet")
        found = sorted(glob.glob(parts_glob))
        if found != sorted(parts):
            raise RuntimeError(
                f"chunk {chunk_cell}: workers reported {len(parts)} parts but "
                f"{len(found)} are on disk at {parts_dir}. Refusing to write a "
                f"partition from a part set that does not match the run."
            )
        hex_values = (
            f"SELECT h AS {h3_col}, value AS {self.value_column}"
            + (", frac" if is_fractions else "")
            + f" FROM read_parquet('{parts_glob}')"
        )

        if is_fractions:
            # Keep nodata rows only for cells that also hold a real class, so the
            # nodata fraction stays explicit on mixed/edge cells (#142) while
            # cells that are *entirely* nodata (e.g. open ocean under a land
            # raster) are dropped rather than emitting a nodata-only row per
            # cell — which would balloon the partition over empty areas.
            select_cols = f"{self.value_column}, frac, {h3_col}{parent_sql}"
            where_sql = ""
            if nodata_codes:
                codes = ", ".join(_fmt_gdal(c) for c in nodata_codes)
                where_sql = (
                    f"WHERE {h3_col} IN ("
                    f"SELECT {h3_col} FROM hex_values "
                    f"WHERE {self.value_column} NOT IN ({codes}))"
                )
            copy_sql = f"""
                COPY (
                    WITH hex_values AS ({hex_values})
                    SELECT {select_cols}
                    FROM hex_values
                    {where_sql}
                ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'zstd')
            """
        else:
            copy_sql = f"""
                COPY (
                    WITH hex_values AS ({hex_values})
                    SELECT {self.value_column}, {h3_col}{parent_sql}
                    FROM hex_values
                ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'zstd')
            """
        self.con.execute(copy_sql)

        # Counted from the file rather than from a frame we no longer hold;
        # parquet keeps the row count in its footer, so this reads no data.
        written = self.con.execute(
            f"SELECT count(*) FROM read_parquet('{output_path}')"
        ).fetchone()[0]
        if written == 0:
            # Every row was filtered out — an all-nodata chunk. Leave no empty
            # partition behind for the merge to find.
            os.remove(output_path)
            return None
        return output_path

    def chunk_cells(self):
        """Ordered [(chunk_cell, h0_cell, h0_index)] — this processor's units of work.

        Thin wrapper over `enumerate_chunk_cells`, which is shared with the
        workflow generator so the fan-out it emits and the list a pod indexes
        into cannot drift apart.
        """
        if self._chunk_cells_cache is None:
            self._chunk_cells_cache = enumerate_chunk_cells(
                self.chunk_resolution,
                h0_subset=self.h0_subset,
                h0_grid_path=self.h0_grid_path,
                con=self.con,
            )
        return self._chunk_cells_cache

    def _chunk_manifest_path(self, chunk_index: int) -> str:
        """Where one chunk records that it ran.

        Beside the parts rather than inside them, because the fact worth
        recording is that the chunk *completed* — which is exactly the case a
        chunk with no data cannot express by writing a part file. `_manifest`
        does not match the `h0=*` glob the merge reads parts through, so the two
        never collide.
        """
        base = self.output_parquet_path.rstrip("/")
        return f"{base}/_manifest/chunk-{chunk_index}.parquet"

    def _record_chunk_completion(self, chunk_index: int, chunk_cell: int,
                                 h0_cell: int, result: Optional[str]) -> None:
        """Write this chunk's completion marker."""
        path = self._chunk_manifest_path(chunk_index)
        if not path.startswith("s3://"):
            os.makedirs(os.path.dirname(path), exist_ok=True)
        self.con.execute(f"""
            COPY (
                SELECT {int(chunk_index)}::BIGINT AS chunk_index,
                       {int(chunk_cell)}::UBIGINT AS chunk_cell,
                       {int(h0_cell)}::UBIGINT AS h0,
                       {"true" if result else "false"}::BOOLEAN AS wrote_data
            ) TO '{path}' (FORMAT PARQUET)
        """)

    def _windowed_source_for(self, geom_wkt: str, margin_deg: float,
                             chunk_cell: int) -> Optional[str]:
        """Localize only the window of the source COG a chunk actually reads.

        Full localization copies the whole file into every pod. That is a fixed
        cost per pod, so total transfer scales with the fan-out: fine at 122 h0
        pods, ruinous at the thousands of pods sub-h0 chunking creates (issue
        #173, lever C). A COG is internally tiled, so GDAL fetches only the
        tiles intersecting the window — the transfer becomes proportional to
        the chunk's area rather than to the number of chunks.

        The window is the chunk's bounding box widened by the same margin the
        overlap test uses, because H3 children can protrude past the parent
        boundary and their pixels have to be in the window or the aggregation
        silently reads nodata there.

        Returns a local path; None to fall back to reading the source directly
        (a chunk straddling the antimeridian, whose window is two disjoint
        longitude ranges a single projWin cannot express); or
        _WINDOW_NO_OVERLAP when the window misses the source entirely, which
        the pruning test can allow through because it deliberately runs a looser
        margin.
        """
        from shapely import wkt as shapely_wkt

        poly = shapely_wkt.loads(geom_wkt)
        minx, miny, maxx, maxy = poly.bounds
        if maxx - minx > 180:
            print("  ℹ chunk straddles the antimeridian; reading the source directly")
            return None

        sxmin, symin, sxmax, symax = self._src_bounds_4326
        lat = min(max(abs(miny), abs(maxy)), 89.0)
        lon_margin = min(margin_deg / max(math.cos(math.radians(lat)), 1e-6), 180.0)

        # Clamp to the source's own extent: projWin outside it either errors or
        # pads with nodata, and either way costs more than it reads.
        wxmin = max(minx - lon_margin, sxmin)
        wxmax = min(maxx + lon_margin, sxmax)
        wymin = max(miny - margin_deg, symin)
        wymax = min(maxy + margin_deg, symax)
        if wxmin >= wxmax or wymin >= wymax:
            return _WINDOW_NO_OVERLAP

        cache_dir = self._window_cache_dir or tempfile.gettempdir()
        os.makedirs(cache_dir, exist_ok=True)
        window_path = os.path.join(cache_dir, f"window_{chunk_cell}.tif")

        print(f"  Windowing source to [{wxmin:.4f},{wymin:.4f},{wxmax:.4f},{wymax:.4f}]...")
        try:
            result = gdal.Translate(
                window_path,
                self.input_path,
                format="GTiff",
                projWin=[wxmin, wymax, wxmax, wymin],
                projWinSRS="EPSG:4326",
                creationOptions=["TILED=YES", "BIGTIFF=IF_SAFER",
                                 "NUM_THREADS=ALL_CPUS"],
                noData=None,
            )
        except RuntimeError as e:
            print(f"  ⚠ Windowed read failed ({e}); reading the source directly")
            return None
        if result is None:
            print("  ⚠ Windowed read produced nothing; reading the source directly")
            return None
        result = None

        if os.path.exists(window_path):
            print(f"  ✓ Window localized: {os.path.getsize(window_path)} bytes")
            return window_path
        return None

    def _chunk_output_path(self, chunk_cell: int, h0_cell: Optional[int] = None) -> str:
        """Where one chunk's parquet goes.

        At chunk_resolution 0 the historical `h0={cell}/data_0.parquet` is
        preserved exactly — that literal path is published in STAC READMEs that
        users copy from, so it is not ours to change. Sub-chunks cannot share
        one filename, so they are written as siblings named by their own cell
        and merged back into `data_0.parquet` by `merge_raster_chunks`, which
        keeps the published layout identical either way.
        """
        if h0_cell is None:
            h0_cell = chunk_cell
        base = f"{self.output_parquet_path.rstrip('/')}/h0={h0_cell}"
        if self.chunk_resolution == 0:
            return f"{base}/data_0.parquet"
        return f"{base}/part-{chunk_cell}.parquet"

    def process_chunk(self, chunk_index: Optional[int] = None) -> Optional[str]:
        """Process one chunk (an h0 cell, or a sub-cell of one) to parquet.

        Generalizes process_h0_region to any chunk_resolution. At
        chunk_resolution 0 it resolves to the same h0 cell, the same geometry
        from the stored grid and the same output path, so the default path is
        unchanged.

        Returns the output parquet path, or None when the chunk has no data.
        """
        if chunk_index is None:
            chunk_index = self.chunk_index
        if chunk_index is None:
            raise ValueError("chunk_index (or h0_index) must be specified")

        if self.chunk_resolution == 0:
            return self.process_h0_region(chunk_index)

        cells = self.chunk_cells()
        if not 0 <= chunk_index < len(cells):
            raise ValueError(
                f"chunk_index {chunk_index} is outside the {len(cells)} chunks at "
                f"chunk-resolution {self.chunk_resolution}"
                + (f" over h0 subset {self.h0_subset}" if self.h0_subset else "")
                + ". The fan-out size and the chunk resolution must match."
            )
        chunk_cell, h0_cell, h0_index = cells[chunk_index]

        print(
            f"\nProcessing chunk {chunk_index} of {len(cells)} "
            f"(res-{self.chunk_resolution} cell {chunk_cell}, "
            f"h0 grid position {h0_index})..."
        )
        print(f"  h0 cell: {describe_h0(h0_cell, self.con)}")

        result = self._process_one_chunk(chunk_cell, h0_cell, chunk_index)
        # Recorded whether or not the chunk wrote anything. A chunk that does
        # not overlap the raster legitimately writes no part file, so the merge
        # step cannot tell "no data here" from "this chunk never ran" by
        # counting parts — and merging the survivors of a partly failed fan-out
        # produces a short dataset with a clean exit (issue #173).
        self._record_chunk_completion(chunk_index, chunk_cell, h0_cell, result)
        return result

    def _process_one_chunk(self, chunk_cell: int, h0_cell: int,
                           chunk_index: int) -> Optional[str]:
        """Aggregate one sub-h0 chunk; returns its parquet path, or None."""
        # A sub-chunk has no row in the h0 grid, so its footprint comes from the
        # cell id itself. _h0_overlaps_raster's antimeridian unwrapping applies
        # unchanged — a res-N cell can straddle +/-180 just as an h0 can.
        geom_wkt = self.con.execute(
            f"SELECT h3_cell_to_boundary_wkt({chunk_cell})"
        ).fetchone()[0]

        from shapely import wkt as _shapely_wkt
        _, cminy, _, cmaxy = _shapely_wkt.loads(geom_wkt).bounds
        chunk_extent = cmaxy - cminy
        window_margin = _H3_PROTRUSION_MARGIN * chunk_extent
        prune_margin = _H3_PRUNE_MARGIN_FACTOR * window_margin

        if not self._h0_overlaps_raster(geom_wkt, margin_deg=prune_margin):
            print(f"  ℹ No overlap between source raster and chunk {chunk_cell}, skipping")
            return None

        if self.method == "warp-centroid":
            # warp-centroid already warps clipped to the chunk's own cutline, so
            # it reads only what it needs without a separate window.
            return self._hex_warp_centroid_h0(geom_wkt, chunk_cell, chunk_index, h0_cell=h0_cell)

        source_path = None
        if self._windowing:
            source_path = self._windowed_source_for(geom_wkt, window_margin, chunk_cell)
            if source_path is _WINDOW_NO_OVERLAP:
                # The looser pruning margin let this chunk through, but its own
                # window misses the raster, so there is provably nothing here.
                print(f"  ℹ Chunk {chunk_cell} window misses the source, skipping")
                return None
        try:
            return self._hex_aggregate_h0(
                chunk_cell, h0_cell=h0_cell, source_path=source_path
            )
        finally:
            # The window is this chunk's alone; a pod that processed several
            # would otherwise accumulate one file per chunk on local disk.
            if source_path and os.path.exists(source_path):
                os.remove(source_path)

    def process_h0_region(self, h0_index: Optional[int] = None) -> Optional[str]:
        """
        Process a single h0 region to H3-indexed parquet.

        Polyfills the h0 cell to its native-resolution H3 children and
        area-weighted-aggregates source-raster values into each cell via
        exactextract. Parent resolutions are added as decoration columns.

        Args:
            h0_index: h0 cell index (0-121), uses self.h0_index if None

        Returns:
            Path to output parquet file, or None if region has no data
        """
        if h0_index is None:
            h0_index = self.h0_index

        if h0_index is None:
            raise ValueError("h0_index must be specified")

        print(f"\nProcessing h0 grid position {h0_index}...")

        # Load h0 polygons to get the geometry using SQL with ST_AsText for WKT
        h0_result = self.con.execute(f"""
            SELECT h0, ST_AsText(geom) as geom_wkt
            FROM read_parquet('{self.h0_grid_path}')
            WHERE i = {h0_index}
        """).fetchdf()

        if len(h0_result) == 0:
            print(f"  ⚠ No h0 region found for index {h0_index}")
            return None

        h0_geom_wkt = h0_result['geom_wkt'].iloc[0]
        h0_cell = h0_result['h0'].iloc[0]

        # Named, not just numbered: a position and a base cell number are
        # indistinguishable at a glance, so this is where a wrong --h0-subset
        # becomes visible (issue #213).
        print(f"  h0 cell: {describe_h0(h0_cell, self.con)}")

        # Skip h0 cells with no overlap with the source raster — avoids
        # running exact_extract over millions of children of a raster that
        # contributes nothing. The antimeridian h0s are stored as polygons
        # spanning ~-178..+177 in planar lat/lon, so their raw envelope both
        # covers the globe (never prunes) AND excludes the +/-180 strip where
        # their data actually lives (would falsely skip a seam raster). Unwrap
        # the longitudes and test the true footprint instead.
        if not self._h0_overlaps_raster(h0_geom_wkt):
            print(f"  ℹ No overlap between source raster and h0 grid position {h0_index}, skipping")
            return None

        # Dispatch by method (issue #84). exact-extract (default) does
        # area-weighted aggregation into native H3 cells via exact_extract;
        # warp-centroid is the opt-in gdal.Warp -> XYZ -> centroid fallback.
        if self.method == "warp-centroid":
            return self._hex_warp_centroid_h0(h0_geom_wkt, h0_cell, h0_index)
        return self._hex_aggregate_h0(h0_cell)

    def _hex_warp_centroid_h0(
        self, h0_geom_wkt: str, chunk_cell: int, h0_index: int,
        h0_cell: Optional[int] = None,
    ) -> Optional[str]:
        """Restored gdal.Warp → XYZ → centroid pipeline (Plan B, opt-in via
        method="warp-centroid").

        Warps the source raster to a grid at the H3 edge pitch, reads pixels
        through DuckDB, and assigns each warped pixel to its H3 cell by
        centroid. Emits one parquet row per warped pixel, NOT per H3 cell —
        consumers must `GROUP BY h<res>` to aggregate. Fast and low-memory,
        but mass-conserving only when warp pitch is finer than source pixel
        pitch (see issue #84).
        """
        from shapely import wkt as shapely_wkt
        src = self._src_bounds_4326
        # h0 bounding box (xmin, ymin, xmax, ymax) from the stored polygon.
        # warp-centroid is an opt-in fallback and is not antimeridian-correct
        # by design (it warps through the planar cutline); the antimeridian-safe
        # path is the default exact-extract method.
        h0_minx, h0_miny, h0_maxx, h0_maxy = shapely_wkt.loads(h0_geom_wkt).bounds

        xyz_file = f"/tmp/raster_{h0_index}.xyz"

        print(f"  warp-centroid: extracting with gdal.Warp at h{self.h3_resolution} pitch...")
        gdal.SetConfigOption('OGR_ENABLE_PARTIAL_REPROJECTION', 'TRUE')

        # Clamp output to the intersection of the h0 cell bbox and the source
        # raster bbox so cropToCutline doesn't allocate a 250-billion-pixel
        # output for fine resolutions.
        inter_xmin = max(src[0], h0_minx)
        inter_ymin = max(src[1], h0_miny)
        inter_xmax = min(src[2], h0_maxx)
        inter_ymax = min(src[3], h0_maxy)

        pixel_size = _h3_res_to_degrees(self.h3_resolution)

        # Canonicalize friendly aliases (e.g. the default "mean" -> "average")
        # to the names GDAL's resampleAlg actually accepts.
        resample_alg = _WARP_RESAMPLER_ALIASES.get(
            self.hex_resampling, self.hex_resampling
        )

        warp_options = gdal.WarpOptions(
            dstSRS='EPSG:4326',
            cutlineWKT=h0_geom_wkt,
            cropToCutline=True,
            outputBounds=(inter_xmin, inter_ymin, inter_xmax, inter_ymax),
            xRes=pixel_size,
            yRes=pixel_size,
            resampleAlg=resample_alg,
            format='XYZ',
            multithread=True,
        )

        result = gdal.Warp(xyz_file, self.input_path, options=warp_options)
        if result is None or not os.path.exists(xyz_file) or os.path.getsize(xyz_file) == 0:
            print(f"  ⚠ No data in region {h0_index}")
            if os.path.exists(xyz_file):
                os.remove(xyz_file)
            return None
        result = None

        try:
            print("  warp-centroid: converting XYZ → H3...")

            # Bound to a local so DuckDB's replacement scan resolves the
            # `FROM xyz_table` reference in the COPY below (looks unused to
            # static linters, hence the noqa).
            xyz_table = self.con.read_csv(  # noqa: F841
                xyz_file,
                delimiter=' ',
                columns={'X': 'FLOAT', 'Y': 'FLOAT', 'Z': 'FLOAT'}
            )

            h3_col = f"h{self.h3_resolution}"
            parent_exprs = []
            for parent_res in sorted(self.parent_resolutions):
                if parent_res < self.h3_resolution:
                    parent_exprs.append(
                        f"h3_latlng_to_cell(Y, X, {parent_res}) AS h{parent_res}"
                    )
            parent_sql = ', ' + ', '.join(parent_exprs) if parent_exprs else ''

            if self.nodata_values:
                vals = ", ".join(_fmt_gdal(v) for v in self.nodata_values)
                where_clause = f"WHERE Z NOT IN ({vals})"
            else:
                where_clause = ""

            output_path = self._chunk_output_path(chunk_cell, h0_cell)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            self.con.execute(f"""
                COPY (
                    SELECT
                        Z AS {self.value_column},
                        h3_latlng_to_cell(Y, X, {self.h3_resolution}) AS {h3_col}
                        {parent_sql}
                    FROM xyz_table
                    {where_clause}
                ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION 'zstd')
            """)

            print(f"  ✓ Wrote: {output_path} (warp-centroid; one row per warped pixel)")
            return output_path
        finally:
            try:
                os.remove(xyz_file)
            except OSError:
                pass

    def process_all_h0_regions(self) -> List[str]:
        """
        Process every h0 region of the grid to H3-indexed parquet, or just the
        ones named by `h0_subset`.

        `h0_subset` used to be ignored here: the loop ran the whole grid, so a
        flag that parsed, validated and printed its restriction quietly did
        nothing on this path, and the run reported `Processing h0 grid position
        0 ... 121` regardless (issue #215). It applied only to
        `enumerate_chunk_cells`, which this method never called. A subset flag
        that appears to have been accepted is the same shape of defect as
        #213 and #218, in a different register.

        The positions come from the grid via the same enumeration the workflow
        generator and `process_chunk` use, rather than `range(122)`, so the
        list cannot drift from the grid it is indexing into.

        Returns:
            List of output parquet file paths
        """
        if self.chunk_resolution:
            # The CLI refuses this combination already; say the same thing to a
            # caller reaching the library directly, rather than aggregating
            # whole h0s into files named as sub-chunk parts.
            raise ValueError(
                "chunk_resolution needs a chunk_index: there is no "
                "process-everything mode for sub-h0 chunks, which exist "
                "precisely so each unit runs in its own pod."
            )

        positions = [
            index for _, _, index in enumerate_chunk_cells(
                0,
                h0_subset=self.h0_subset,
                h0_grid_path=self.h0_grid_path,
                con=self.con,
            )
        ]
        if self.h0_subset:
            print(f"Restricted to {len(positions)} of the grid's h0 positions: "
                  f"{positions}")

        output_files = []
        for h0_index in positions:
            try:
                output_file = self.process_h0_region(h0_index)
                if output_file:
                    output_files.append(output_file)
            except Exception as e:
                print(f"  ✗ Error processing h0 {h0_index}: {e}")

        print(f"\n✓ Processed {len(output_files)} h0 regions")
        return output_files


def create_cog(
    input_path: str,
    output_path: str,
    compression: str = "deflate",
    blocksize: int = 512,
    overviews: bool = True,
    resampling: str = "nearest",
    **kwargs
) -> str:
    """
    Create a Cloud-Optimized GeoTIFF.

    Convenience function that wraps RasterProcessor.create_cog().

    Args:
        input_path: Path to input raster file
        output_path: Path for output COG file
        compression: Compression method (deflate, lzw, zstd, etc.)
        blocksize: Internal tile size
        overviews: Whether to create overview pyramids
        resampling: Resampling method
        **kwargs: Additional arguments passed to RasterProcessor

    Returns:
        Path to created COG file
    """
    processor = RasterProcessor(
        input_path=input_path,
        output_cog_path=output_path,
        compression=compression,
        blocksize=blocksize,
        resampling=resampling,
        **kwargs
    )

    return processor.create_cog()
