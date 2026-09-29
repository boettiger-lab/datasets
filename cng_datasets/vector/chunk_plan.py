"""
Cells-per-chunk planning for the vector hex fan-out (issue #124).

A hex pod's memory and runtime follow the H3 cells of the features it is
given, not how many features there are. Per-feature cell counts are extremely
skewed — on IUCN ranges at res 6 the median feature is 345 cells and the
largest 2.5 M — so no fixed features-per-chunk balances a real layer. Measured
on three published layers, the largest fixed-count chunk ran 2.2-2.4x the mean
at the default size and 8-36x when the count was lowered by hand; a
cells-per-chunk budget brought all three to 1.0-1.2x (see #124).

The plan is computed once, after convert, over the GeoParquet the hex pods
will read. It cuts that file into contiguous row ranges, starting a new chunk
whenever *either* the cells budget or the features-per-chunk cap would be
exceeded. The feature cap is there because pass 2 has a per-feature cost too
(one batch query per ``intermediate_chunk_size`` rows), so a cells budget alone
would pack millions of points into one chunk; with both caps the plan only
ever *splits* the chunks the fixed rule would have made.

Ranges are contiguous because the hex pod selects its rows with
``LIMIT/OFFSET`` in file order, and the synthetic ``_fid`` is derived from the
row offset — so a plan changes where chunks start, never which id a row gets.
"""

import math
import os
from typing import List, NamedTuple, Optional, Sequence, Tuple

import duckdb
import numpy as np

from cng_datasets.storage.s3 import configure_s3_credentials
from .h3_tiling import (
    _native_res_sql,
    _polygon_cells_sql,
    find_geometry_column,
    setup_duckdb_connection,
)

# Default cells per hex chunk. Chosen from the #124 measurements: at 5 M the
# three measured layers balanced to 1.0-1.2x mean with at most one feature
# larger than the budget on its own. The budget is a floor on how finely the
# work is split, not a memory guarantee: a single feature larger than it still
# gets a chunk of its own.
DEFAULT_CELLS_PER_CHUNK = 5_000_000

# The fixed rule's features per chunk (#144), which the plan never exceeds.
DEFAULT_MAX_FEATURES_PER_CHUNK = 1000

# A hex pod's peak memory, as a function of its chunk's estimated cells.
# Measured 2026-09-29 on EPA L3 ecoregions at res 10: 97 pods of a planned
# build, peak read from the cgroup's memory.peak, least-squares fit (residual
# SD 0.71 GiB; vertex complexity is the rest). At the default budget this is
# ~2.4 GiB, far inside the default 8Gi, so the budget is set by balancing
# runtime rather than memory — memory binds only on single features larger than
# the budget, which is what the warning below is for (#124).
PEAK_BASE_BYTES = int(0.74 * 2**30)
PEAK_BYTES_PER_CELL = 354

# Where Kubernetes reads a container's termination message. The plan reports
# its chunk count here so the workflow can size the hex Job to it.
TERMINATION_LOG = "/dev/termination-log"


class Chunk(NamedTuple):
    chunk_id: int
    row_offset: int
    row_count: int
    est_cells: float


class ChunkPlan(NamedTuple):
    chunks: List[Chunk]
    cells_budget: float          # the budget actually used
    requested_budget: float      # the budget asked for
    max_features: int            # the feature cap actually used
    total_rows: int
    total_cells: float
    oversized: int               # features larger than the budget on their own


def feature_cells_sql(geom_expr: str, h3_resolution: int,
                      resolution_by_area=None) -> str:
    """Estimated H3 cells a feature produces, for any geometry type.

    Polygons use the #107 guard's area estimate; lines their geodesic length
    over the average edge length; points one cell each. Never below 1, and a
    NULL or NaN estimate counts as 1 rather than poisoning the running sum.
    """
    res = _native_res_sql(geom_expr, h3_resolution, resolution_by_area)
    gtype = f"ST_GeometryType({geom_expr})"
    raw = (
        f"CASE WHEN {gtype} = 'POINT' THEN 1 "
        f"WHEN {gtype} = 'MULTIPOINT' THEN ST_NPoints({geom_expr}) "
        f"WHEN {gtype} IN ('LINESTRING', 'MULTILINESTRING') THEN "
        f"ST_Length_Spheroid(ST_FlipCoordinates({geom_expr})) / "
        f"h3_get_hexagon_edge_length_avg({res}, 'm') "
        f"ELSE {_polygon_cells_sql(geom_expr, h3_resolution, resolution_by_area)} END"
    )
    return (f"CASE WHEN ({raw}) IS NULL OR isnan(({raw})::DOUBLE) THEN 1 "
            f"ELSE GREATEST(1, ({raw})::DOUBLE) END")


def cut_chunks(cells: Sequence[float], cells_budget: float,
               max_features: int) -> List[Tuple[int, int, float]]:
    """Contiguous (row_offset, row_count, est_cells) ranges.

    A new chunk starts before a row that would take the current one past the
    cells budget or the feature cap. A row larger than the budget on its own
    therefore gets a chunk of its own rather than being split — a feature is
    the smallest unit a pod can be given.
    """
    out = []
    start, acc, n, i = 0, 0.0, 0, 0
    for c in _floats(cells):
        if n and (acc + c > cells_budget or n >= max_features):
            out.append((start, n, acc))
            start, acc, n = i, 0.0, 0
        acc += c
        n += 1
        i += 1
    if n:
        out.append((start, n, acc))
    return out


def _floats(cells, block: int = 1 << 20):
    """Python floats from *cells*, a block at a time.

    A numpy array is iterated through ``tolist()`` in blocks: element access on
    the array itself is several times slower, and converting all of it at once
    would hold every row as a Python float (~1.2 GB for 38 M rows).
    """
    if isinstance(cells, np.ndarray):
        for lo in range(0, len(cells), block):
            yield from cells[lo:lo + block].tolist()
    else:
        yield from cells


def plan_chunks(cells: Sequence[float], cells_budget: float = DEFAULT_CELLS_PER_CHUNK,
                max_chunks: Optional[int] = None,
                max_features: Optional[int] = None) -> ChunkPlan:
    """Cut *cells* (per-row estimates, in file order) into a chunk plan.

    With *max_chunks*, the plan is made to fit: the feature cap is first
    raised to the fixed rule's ``ceil(rows / max_chunks)`` if needed, and the
    cells budget is then raised until the chunk count fits. Either raise is
    visible in the returned plan, so the caller can report it.
    """
    cells = np.asarray(cells, dtype=np.float64)
    total_rows = len(cells)
    if cells_budget <= 0:
        raise ValueError(f"cells_budget must be positive, got {cells_budget}")
    if max_features is None:
        max_features = DEFAULT_MAX_FEATURES_PER_CHUNK
    if max_chunks is not None:
        if max_chunks < 1:
            raise ValueError(f"max_chunks must be at least 1, got {max_chunks}")
        max_features = max(max_features, math.ceil(total_rows / max_chunks))

    budget = float(cells_budget)
    ranges = cut_chunks(cells, budget, max_features)
    # Terminates: once the budget exceeds the total, only the feature cap cuts,
    # and it alone fits max_chunks by construction above.
    while max_chunks is not None and len(ranges) > max_chunks:
        budget *= max(1.05, len(ranges) / max_chunks)
        ranges = cut_chunks(cells, budget, max_features)

    chunks = [Chunk(i, off, cnt, est) for i, (off, cnt, est) in enumerate(ranges)]
    return ChunkPlan(
        chunks=chunks,
        cells_budget=budget,
        requested_budget=float(cells_budget),
        max_features=max_features,
        total_rows=total_rows,
        total_cells=float(cells.sum()),
        oversized=int((cells > budget).sum()),
    )


def predicted_peak_bytes(est_cells: float) -> float:
    """A hex pod's predicted peak memory for a chunk of *est_cells*."""
    return PEAK_BASE_BYTES + PEAK_BYTES_PER_CELL * est_cells


def _connect() -> duckdb.DuckDBPyConnection:
    con = setup_duckdb_connection()
    configure_s3_credentials(con)
    return con


def estimate_row_cells(con: duckdb.DuckDBPyConnection, input_url: str,
                       h3_resolution: int, resolution_by_area=None) -> np.ndarray:
    """Per-row cell estimates for *input_url*, in the order the hex pods read it.

    The hex pod selects rows with `LIMIT/OFFSET` over the same `read_parquet`
    scan, and DuckDB preserves insertion order by default, so row i here is row
    i there.
    """
    con.execute(f"CREATE OR REPLACE VIEW plan_source AS "
                f"SELECT * FROM read_parquet('{input_url}')")
    geom = find_geometry_column(con, "plan_source")
    geom_expr = f'"{geom}"'
    return con.execute(
        f"SELECT {feature_cells_sql(geom_expr, h3_resolution, resolution_by_area)} AS cells "
        f"FROM plan_source"
    ).fetchnumpy()["cells"].astype(np.float64)


def write_plan(con: duckdb.DuckDBPyConnection, plan: ChunkPlan, output_url: str) -> None:
    """Write *plan* as parquet: one row per chunk."""
    import pandas as pd

    frame = pd.DataFrame(plan.chunks, columns=list(Chunk._fields))
    con.register("plan_frame", frame)
    try:
        if not output_url.startswith(("s3://", "http://", "https://")):
            os.makedirs(os.path.dirname(output_url) or ".", exist_ok=True)
        con.execute(f"""
            COPY (SELECT chunk_id::INTEGER AS chunk_id, row_offset::BIGINT AS row_offset,
                         row_count::BIGINT AS row_count, est_cells::DOUBLE AS est_cells
                  FROM plan_frame ORDER BY chunk_id)
            TO '{output_url}' (FORMAT PARQUET, KV_METADATA {{
                cells_budget: '{plan.cells_budget:.0f}',
                requested_budget: '{plan.requested_budget:.0f}',
                max_features: '{plan.max_features}',
                total_rows: '{plan.total_rows}',
                total_cells: '{plan.total_cells:.0f}'
            }})
        """)
    finally:
        con.unregister("plan_frame")


def read_plan_chunk(con: duckdb.DuckDBPyConnection, plan_url: str,
                    chunk_id: int) -> Tuple[Optional[Chunk], int]:
    """(the chunk, or None if *chunk_id* is past the plan; the plan's chunk count)."""
    n = con.execute(f"SELECT count(*) FROM read_parquet('{plan_url}')").fetchone()[0]
    row = con.execute(
        f"SELECT chunk_id, row_offset, row_count, est_cells "
        f"FROM read_parquet('{plan_url}') WHERE chunk_id = {int(chunk_id)}"
    ).fetchone()
    return (Chunk(*row) if row else None), n


def report(plan: ChunkPlan, max_chunks: Optional[int],
           hex_memory_bytes: Optional[float] = None) -> List[str]:
    """Human-readable summary lines, including anything that was raised."""
    sizes = [c.est_cells for c in plan.chunks]
    mean = (sum(sizes) / len(sizes)) if sizes else 0.0
    lines = [
        f"  Rows: {plan.total_rows:,}; estimated cells: {plan.total_cells:,.0f}",
        f"  Chunks: {len(plan.chunks):,} (budget {plan.cells_budget:,.0f} cells, "
        f"at most {plan.max_features:,} features each)",
    ]
    if sizes:
        lines.append(f"  Largest chunk: {max(sizes):,.0f} cells "
                     f"({max(sizes) / mean:.1f}x the mean)" if mean else
                     f"  Largest chunk: {max(sizes):,.0f} cells")
    if plan.cells_budget > plan.requested_budget:
        lines.append(
            f"  ⚠ The {plan.requested_budget:,.0f}-cell budget needed more than "
            f"{max_chunks:,} chunks, so it was raised to {plan.cells_budget:,.0f} "
            f"to fit --max-completions. Raise --max-completions to keep smaller "
            f"chunks (past ~200, use --backend armada).")
    if plan.oversized:
        lines.append(
            f"  ℹ {plan.oversized:,} feature(s) exceed the budget on their own; "
            f"each is a chunk by itself, the finest split possible.")
    if hex_memory_bytes:
        # 90%: the fit's residual SD is ~0.7 GiB, so a prediction at the
        # limit is roughly a coin flip.
        risky = [c for c in plan.chunks
                 if predicted_peak_bytes(c.est_cells) > 0.9 * hex_memory_bytes]
        if risky:
            worst = max(risky, key=lambda c: c.est_cells)
            lines.append(
                f"  ⚠ {len(risky):,} chunk(s) are predicted to peak near or over the "
                f"{hex_memory_bytes / 2**30:.0f} GiB hex memory limit; the largest, "
                f"chunk {worst.chunk_id} (rows {worst.row_offset:,}+{worst.row_count:,}, "
                f"~{worst.est_cells:,.0f} cells), at "
                f"~{predicted_peak_bytes(worst.est_cells) / 2**30:.1f} GiB. A chunk "
                f"cannot be split below one feature: raise --hex-memory, or hex "
                f"the largest features coarser (--resolution-by-area).")
    return lines


def run_plan(input_url: str, output_url: str, h3_resolution: int = 10,
             resolution_by_area=None,
             cells_per_chunk: float = DEFAULT_CELLS_PER_CHUNK,
             max_chunks: Optional[int] = None,
             max_features: Optional[int] = None,
             termination_log: Optional[str] = TERMINATION_LOG,
             hex_memory_bytes: Optional[float] = None) -> ChunkPlan:
    """Estimate, cut and write the plan; report the chunk count to Kubernetes."""
    con = _connect()
    try:
        print(f"Planning hex chunks for {input_url}...")
        cells = estimate_row_cells(con, input_url, h3_resolution, resolution_by_area)
        if len(cells) == 0:
            raise ValueError(f"{input_url} has no rows to plan")
        plan = plan_chunks(cells, cells_per_chunk, max_chunks=max_chunks,
                           max_features=max_features)
        for line in report(plan, max_chunks, hex_memory_bytes):
            print(line)
        write_plan(con, plan, output_url)
        print(f"  ✓ Wrote plan: {output_url}")
    finally:
        con.close()
    # The workflow sizes the hex Job from this. Written last, so it only ever
    # reports a plan that exists.
    if termination_log and os.path.exists(termination_log):
        try:
            with open(termination_log, "w") as f:
                f.write(f"chunks={len(plan.chunks)}\n")
        except OSError:
            pass
    return plan


__all__ = [
    "DEFAULT_CELLS_PER_CHUNK", "DEFAULT_MAX_FEATURES_PER_CHUNK", "Chunk", "ChunkPlan",
    "cut_chunks", "plan_chunks", "feature_cells_sql", "estimate_row_cells",
    "predicted_peak_bytes", "PEAK_BASE_BYTES", "PEAK_BYTES_PER_CELL",
    "write_plan", "read_plan_chunk", "run_plan",
]
