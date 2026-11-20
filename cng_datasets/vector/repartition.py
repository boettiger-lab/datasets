"""
Repartition vector datasets by H3 resolution 0 cells.

Consolidates chunked processing outputs into h0-partitioned format
for efficient spatial querying.
"""

import math
import os
import shutil
import subprocess
import duckdb
from cng_datasets.duckdb_memory import memory_bytes, to_duckdb_memory_limit
from cng_datasets.hex_checks import assert_h3_columns_unsigned
from cng_datasets.storage.s3 import configure_s3_credentials
from ..provenance import kv_metadata_sql
from .geoparquet import covering_columns

# Rows sorted at once when ordering one h0 partition (#339). A 50M-row range
# at memory_limit 40GiB kept scratch in single-digit GB on the 700M-row PAD-US
# combined partition (35 attributes). The default scales that ratio to the
# memory limit, so a smaller pod sorts smaller ranges; without a limit, or
# above 40GiB, it is 50M.
DEFAULT_SORT_RANGE_ROWS = 50_000_000
SORT_RANGE_BYTES_PER_ROW = (40 * 2 ** 30) // DEFAULT_SORT_RANGE_ROWS


def default_sort_range_rows(memory_limit) -> int:
    """Rows to sort at once under DuckDB *memory_limit* (#339)."""
    limit = memory_bytes(memory_limit)
    if limit is None:
        return DEFAULT_SORT_RANGE_ROWS
    return max(1, min(DEFAULT_SORT_RANGE_ROWS, limit // SORT_RANGE_BYTES_PER_ROW))


# DuckDB threads when the pod's cpu is unknown: the hex step's fallback worker
# count for the same situation (raster/cog.py, #195).
UNKNOWN_CPU_THREADS = 8


def repartition_threads(con) -> int:
    """DuckDB threads for this pod: its cpu request, else at most 8 (#339)."""
    from cng_datasets.h3_chunks import _cpu_request
    cpus = _cpu_request()
    if cpus:
        threads = max(1, math.ceil(cpus))
        source = "the pod's cpu"
    else:
        threads = min(UNKNOWN_CPU_THREADS,
                      int(con.execute("SELECT current_setting('threads')").fetchone()[0]))
        source = "pod cpu unknown; set CNG_POD_CPU"
    print(f"Setting DuckDB threads={threads} ({source})")
    return threads


def _sql_literal(value) -> str:
    """An id range bound as SQL: numbers as is, anything else quoted."""
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def repartition_by_h0(
    chunks_dir: str,
    output_dir: str,
    source_parquet: str = None,
    cleanup: bool = True,
    memory_limit: str = None,
    sort_range_rows: int = None,
) -> None:
    """
    Repartition chunks by h0 for efficient spatial querying.
    Joins back attribute columns from source parquet (without geometry).

    Args:
        chunks_dir: S3 URL or local path to chunks directory
        output_dir: S3 URL or local path to output directory
        source_parquet: S3 URL to original parquet (for joining attributes)
        cleanup: Whether to remove chunks directory after repartitioning
        memory_limit: DuckDB memory limit (e.g. '27GiB'). Falls back to
            DUCKDB_MEMORY_LIMIT env var. If neither is set, DuckDB auto-detects
            (which may ignore container cgroup limits).
        sort_range_rows: Most rows sorted at once (default: the
            CNG_SORT_RANGE_ROWS env var, else default_sort_range_rows of the
            memory limit, 50M rows per 40GiB and at most 50M). A
            larger h0 partition is sorted in contiguous id ranges of about this
            many rows, which are then streamed in order into its one file, so
            scratch space tracks the range rather than the partition (#339).
    """
    print(f"Repartitioning chunks from {chunks_dir} to {output_dir}")

    # Set up DuckDB connection with S3 credentials
    con = duckdb.connect()
    configure_s3_credentials(con)
    con.execute('SET preserve_insertion_order=false')  # saves RAM
    requested_limit = memory_limit or os.environ.get('DUCKDB_MEMORY_LIMIT')
    if requested_limit:
        # DuckDB rejects the Kubernetes spelling of a memory quantity, and this
        # value usually arrives from a manifest (issue #217).
        effective_limit = to_duckdb_memory_limit(requested_limit)
        suffix = "" if effective_limit == requested_limit else f" (from {requested_limit})"
        print(f"Setting DuckDB memory_limit={effective_limit}{suffix}")
        con.execute(f"SET memory_limit='{effective_limit}'")
    else:
        effective_limit = None
    # DuckDB counts the host's cores, not the pod's: 48 threads in a 4-cpu pod.
    # Every thread holds its own sort and join buffers, so under a small
    # memory_limit the same COPY passes or runs out of memory by chance; at
    # the pod's own count it passes (measured on the #339 MRE at
    # memory_limit 3GiB). NRP enforces no cpu quota, so the generated manifest
    # states the request as CNG_POD_CPU; with neither, at most
    # UNKNOWN_CPU_THREADS.
    con.execute(f"SET threads={repartition_threads(con)}")
    con.execute('SET http_timeout=1200')
    con.execute('SET http_retries=30')
    con.execute('SET arrow_large_buffer_size=true')

    # Create local temporary directory
    local_dir = '/tmp/hex'
    os.makedirs(local_dir, exist_ok=True)

    # Read chunks (sparse format: ID_column, h10, h9, h8, h0)
    try:
        chunk_cols = [row[0] for row in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{chunks_dir}/*.parquet')"
        ).fetchall()]
    except Exception as e:
        raise RuntimeError(
            f"No parquet files found in chunks directory '{chunks_dir}'. "
            f"The hex job may have produced no output (all cells empty?). "
            f"Original error: {e}"
        ) from e

    # Identify ID column in chunks
    # Prioritize _cng_fid (standard from convert_to_parquet), then _fid (fallback), then other non-h3 columns
    chunk_id_col = None
    priority_ids = ['_cng_fid', '_fid']

    # First check for priority IDs
    for priority_id in priority_ids:
        if priority_id in chunk_cols:
            chunk_id_col = priority_id
            break

    # If no priority ID found, look for any non-h3 column
    if not chunk_id_col:
        for col in chunk_cols:
            if not col.startswith('h') or not col[1:].isdigit():
                chunk_id_col = col
                break

    if not chunk_id_col:
        raise ValueError(f"Could not identify ID column in chunks. Columns: {chunk_cols}")

    print(f"Chunks ID column: {chunk_id_col}")

    # Finest H3 column in the chunks: within a feature, rows are ordered by it
    h_cols = [c for c in chunk_cols if c.startswith('h') and c[1:].isdigit()]
    finest_h = max(h_cols, key=lambda c: int(c[1:]))

    # If source parquet provided, join back all attributes (except geometry)
    if source_parquet:
        print(f'Joining attributes from {source_parquet}...')

        desc = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{source_parquet}')").fetchdf()
        source_cols = desc['column_name'].tolist()

        # Use helper to find geometry column (case-insensitive or by GEOMETRY type)
        col_lower_map = {col.lower(): col for col in source_cols}
        col_types = dict(zip(desc['column_name'], desc['column_type']))
        geom_col = None
        for name in ['geometry', 'geom', 'shape']:
            if name in col_lower_map:
                geom_col = col_lower_map[name]
                break
        if not geom_col:
            for col, typ in col_types.items():
                if typ.upper().startswith('GEOMETRY'):
                    geom_col = col
                    break

        # Verify the chunk ID column exists in source
        if chunk_id_col not in source_cols:
            raise ValueError(f"Chunk ID column '{chunk_id_col}' not found in source parquet. Source columns: {source_cols}")

        # Select all columns except geometry, and except the GeoParquet bbox
        # covering column (#340), which describes the geometry and leaves with it
        covering = set(covering_columns(con, source_parquet))
        attr_cols = [c for c in source_cols if c != geom_col and c not in covering]

        print(f'  Joining on column: {chunk_id_col}')
        print(f'  Adding {len(attr_cols) - 1} attribute columns')

        # A view of just the attribute columns: the hex output carries no geometry
        quoted = ', '.join(f'"{c}"' for c in attr_cols)
        con.execute(f"CREATE OR REPLACE VIEW _source_attrs AS SELECT {quoted} FROM read_parquet('{source_parquet}')")
        join_sql = (
            f"SELECT s.*, c.* EXCLUDE (\"{chunk_id_col}\")"
            f" FROM read_parquet('{chunks_dir}/*.parquet') c"
            f" INNER JOIN _source_attrs s ON c.\"{chunk_id_col}\" = s.\"{chunk_id_col}\""
            f" WHERE c.h0 = {{h0}}"
            f" AND c.\"{chunk_id_col}\" BETWEEN {{lo}} AND {{hi}}"
            f" AND s.\"{chunk_id_col}\" BETWEEN {{lo}} AND {{hi}}"
        )

        # Completeness guard (issue #170): the k8s hex Job is an indexed Job of
        # `max-completions` chunks of `chunk-size` rows, so it only ever covers
        # the first `max-completions × chunk-size` features. When the generator
        # could not count the source up front it falls back to a default cap
        # (e.g. 200k), and every downstream presence check still passes even
        # though features past the cap were silently never hexed — only a
        # post-build COUNT(DISTINCT id) catches it. Assert here, at the pipeline's
        # natural endpoint (we hold both the hex output and the source), that
        # every source feature with a non-null geometry made it into the hex. A
        # shortfall is fatal: it turns silent data loss into a failed run.
        #
        # Null-geometry features are excluded because they legitimately produce
        # no H3 cell (issue #169). Set CNG_SKIP_COMPLETENESS_CHECK=1 to bypass
        # (e.g. a build with known-degenerate geometries that drop to 0 cells).
        if os.environ.get('CNG_SKIP_COMPLETENESS_CHECK') not in ('1', 'true', 'True'):
            geom_filter = f' WHERE "{geom_col}" IS NOT NULL' if geom_col else ''
            source_features = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{source_parquet}'){geom_filter}"
            ).fetchone()[0]
            hexed_features = con.execute(
                f'SELECT COUNT(DISTINCT "{chunk_id_col}")'
                f" FROM read_parquet('{chunks_dir}/*.parquet')"
            ).fetchone()[0]
            geom_note = ' with non-null geometry' if geom_col else ''
            print(
                f'  Completeness check: {hexed_features:,} of {source_features:,} '
                f'source features{geom_note} present in hex output'
            )
            if hexed_features < source_features:
                missing = source_features - hexed_features
                raise RuntimeError(
                    f"Hex output is incomplete: {hexed_features:,} of "
                    f"{source_features:,} source features{geom_note} were hexed "
                    f"({missing:,} missing). Likely causes, most likely first:\n"
                    f"  1. A hex index failed or never ran: check the hex Job's "
                    f"failedIndexes (`kubectl get job <name>-hex -o "
                    f"jsonpath='{{.status.failedIndexes}}'`) and re-run those "
                    f"indexes (#258).\n"
                    f"  2. The fan-out was too small for the source: completions × "
                    f"chunk size < {source_features:,} (issue #170). Re-run the hex "
                    f"step with a larger --max-completions.\n"
                    f"  3. Genuinely degenerate geometry that produces 0 H3 cells: "
                    f"set CNG_SKIP_COMPLETENESS_CHECK=1 to proceed.\n"
                    f"Chunks left in place at '{chunks_dir}' for inspection."
                )
    else:
        print('No source parquet provided, proceeding without attribute join')
        join_sql = (
            f"SELECT * FROM read_parquet('{chunks_dir}/*.parquet') WHERE h0 = {{h0}}"
            f" AND \"{chunk_id_col}\" BETWEEN {{lo}} AND {{hi}}"
        )

    # Plan each h0 partition as contiguous id ranges of about sort_range_rows
    # rows (#339). One COPY ... ORDER BY over a whole partition makes DuckDB
    # sort every joined row, attributes included, before writing any of them,
    # so its scratch grew with the partition's row count: 700M rows of PAD-US
    # combined spilled past 200Gi for a 1.4 GB file. Sorted range by range and
    # streamed in order, the file is the same, rows and order, and scratch
    # tracks one range. Each feature goes to the range its first row falls
    # in, so a feature's rows are never split between ranges and a range holds
    # at most the budget plus the rest of its last feature.
    range_rows = int(sort_range_rows or os.environ.get('CNG_SORT_RANGE_ROWS')
                     or default_sort_range_rows(effective_limit))
    if range_rows < 1:
        raise ValueError(f"sort_range_rows must be positive, got {range_rows}")
    plan = con.execute(f"""
        WITH per_id AS (
            SELECT h0, "{chunk_id_col}" AS id, count(*) AS n
            FROM read_parquet('{chunks_dir}/*.parquet') GROUP BY ALL),
        ranged AS (
            SELECT h0, id, n,
                   sum(n) OVER (PARTITION BY h0 ORDER BY id
                                ROWS UNBOUNDED PRECEDING) - n AS first_row
            FROM per_id)
        -- A feature belongs to the range its first row falls in.
        SELECT h0, first_row // {range_rows} AS rid, min(id), max(id), sum(n)::BIGINT
        FROM ranged
        GROUP BY h0, rid ORDER BY h0, rid
    """).fetchall()
    if not plan:
        raise RuntimeError(
            f"No parquet files found in chunks directory '{chunks_dir}'. "
            f"The hex job may have produced no output (all cells empty?)."
        )
    partitions = {}
    for h0, _rid, lo, hi, n in plan:
        partitions.setdefault(h0, []).append((lo, hi, n))

    print(f'Writing {len(partitions)} h0 partitions one at a time (bounded memory, '
          f'at most ~{range_rows:,} rows sorted at once)...')

    # Determine rclone output path once
    if output_dir.startswith('s3://'):
        parts = output_dir.replace('s3://', '').split('/', 1)
        rclone_output = f'nrp:{parts[0]}/{parts[1].rstrip("/")}' if len(parts) == 2 else f'nrp:{parts[0]}'
    else:
        rclone_output = None

    def ordered(h0, lo, hi):
        # ORDER BY the row id within each h0 partition (issue #103) so parquet
        # row-group zonemaps can prune by feature identity: a plain
        # `WHERE _cng_fid IN (...)` then skips the partitions/row-groups whose
        # [min,max] excludes the targets instead of scanning every row. Convert
        # numbers _cng_fid in Hilbert order (issue #276), so the same order
        # also groups rows by location; ordering each feature's cells by the
        # finest H3 column keeps them compact and their attributes compress.
        # Wrap in a subquery so ORDER BY references the unambiguous output
        # column (the join exposes the id on both sides).
        lo, hi = _sql_literal(lo), _sql_literal(hi)
        return (f"SELECT * FROM ({join_sql.format(h0=h0, lo=lo, hi=hi)})"
                f" ORDER BY \"{chunk_id_col}\", \"{finest_h}\"")

    any_written = False
    for h0, ranges in partitions.items():
        local_partition = os.path.join(local_dir, f'h0={h0}')
        os.makedirs(local_partition, exist_ok=True)
        local_file = os.path.join(local_partition, 'data_0.parquet')
        expected = sum(n for _, _, n in ranges)

        if len(ranges) == 1:
            (lo, hi, _), = ranges
            con.execute(
                f"COPY ({ordered(h0, lo, hi)}) TO '{local_file}'"
                f" (FORMAT PARQUET, COMPRESSION ZSTD{kv_metadata_sql()})"
            )
        else:
            print(f'  h0={h0}: {expected:,} rows, sorted in {len(ranges)} id ranges')
            # No '=' in this path: DuckDB would read it as a hive key and add
            # a column to the partition.
            sorted_dir = os.path.join(local_dir, f'_ranges_{h0}')
            os.makedirs(sorted_dir, exist_ok=True)
            range_files = []
            for i, (lo, hi, _) in enumerate(ranges):
                range_file = os.path.join(sorted_dir, f'range_{i:05d}.parquet')
                con.execute(f"COPY ({ordered(h0, lo, hi)}) TO '{range_file}'"
                            f" (FORMAT PARQUET, COMPRESSION ZSTD)")
                range_files.append(range_file)
            # Ranges are disjoint and ascending in id, so reading them back in
            # list order, with insertion order kept, is the sorted partition.
            file_list = ', '.join(f"'{f}'" for f in range_files)
            con.execute('SET preserve_insertion_order=true')
            try:
                con.execute(
                    f"COPY (SELECT * FROM read_parquet([{file_list}], hive_partitioning=false))"
                    f" TO '{local_file}'"
                    f" (FORMAT PARQUET, COMPRESSION ZSTD{kv_metadata_sql()})"
                )
            finally:
                con.execute('SET preserve_insertion_order=false')
            shutil.rmtree(sorted_dir)

        # Every row the plan counted is in the file, before anything is
        # uploaded: a join that dropped rows would otherwise publish a short
        # partition with a clean exit.
        written = con.execute(
            f"SELECT count(*) FROM read_parquet('{local_file}')").fetchone()[0]
        if written != expected:
            raise RuntimeError(
                f"h0={h0}: wrote {written:,} rows, but the chunks hold {expected:,}. "
                f"More rows means '{chunk_id_col}' is not unique in the source "
                f"parquet; fewer means chunk ids are missing from it. Chunks "
                f"left in place at '{chunks_dir}'.")

        if rclone_output:
            subprocess.run(
                ['rclone', 'copy', local_partition, f'{rclone_output}/h0={h0}/',
                 '--transfers', '32',
                 '--s3-upload-concurrency', '16',
                 '--s3-chunk-size', '64M'],
                check=True,
            )
        else:
            dest = os.path.join(output_dir.rstrip('/'), f'h0={h0}')
            shutil.copytree(local_partition, dest, dirs_exist_ok=True)

        shutil.rmtree(local_partition)
        any_written = True
        print(f'  h0={h0} done')

    if not any_written:
        raise RuntimeError(
            f"No partitions written — all chunks appear to be empty. "
            f"Check that the hex job in '{chunks_dir}' produced non-empty output."
        )

    # Assert H3 index columns are UBIGINT through the consumer (hive glob) path,
    # before cleaning up the chunks. Raises on a non-UBIGINT h{N>=1} column
    # (issue #102), leaving chunks intact for debugging if it fails.
    hex_glob = f"{output_dir.rstrip('/')}/h0=*/data_0.parquet"
    print(f'Asserting H3 columns are UBIGINT via {hex_glob}...')
    assert_h3_columns_unsigned(lambda sql: con.execute(sql).fetchall(), hex_glob)

    print('Cleaning up local directory...')
    shutil.rmtree(local_dir, ignore_errors=True)

    print('✓ Repartitioning complete!')

    # Clean up chunks directory if requested
    if cleanup and chunks_dir.startswith('s3://'):
        print('Removing chunks directory from S3...')
        # Extract bucket and path from s3://bucket/path
        parts = chunks_dir.replace('s3://', '').split('/', 1)
        if len(parts) == 2:
            bucket, path = parts
            rclone_path = f'nrp:{bucket}/{path}'
        else:
            rclone_path = f'nrp:{parts[0]}'

        try:
            result = subprocess.run(
                ['rclone', 'purge', rclone_path],
                capture_output=True,
                text=True,
                timeout=300
            )
            if result.returncode == 0:
                print('✓ Chunks directory removed successfully')
            else:
                print(f'⚠ rclone cleanup warning: {result.stderr}')
        except Exception as e:
            print(f'⚠ Error during cleanup: {e}')

    print('✓ All done!')
