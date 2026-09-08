"""
Merge sub-h0 raster hex chunks back into one file per h0 partition.

Sub-h0 chunking (issue #173) makes the unit of work a res-N descendant of an
h0 base cell rather than the whole h0, which is what keeps a hex pod's peak
memory off the densest h0's cell count. The cost is that several pods now write
into the same ``h0={cell}/`` partition, and that partition's single-file layout
(``h0={cell}/data_0.parquet``) is published in STAC READMEs users copy literal
paths from. This step restores it: many ``part-{cell}.parquet`` in, one
``data_0.parquet`` out, so the shape of the published dataset does not depend on
how finely the build was chunked.

Merging is per h0 and streamed through DuckDB rather than concatenated in
memory, so it does not re-materialise the very thing sub-chunking avoided.
"""

import os
import shutil
import subprocess
from typing import Optional

import ibis

from cng_datasets.hex_checks import assert_h3_columns_unsigned
from cng_datasets.storage.s3 import configure_s3_credentials


def _rclone_dest(output_dir: str) -> Optional[str]:
    """Map an s3:// output dir to an rclone remote path, or None if local."""
    if not output_dir.startswith("s3://"):
        return None
    parts = output_dir.replace("s3://", "").split("/", 1)
    if len(parts) == 2:
        return f'nrp:{parts[0]}/{parts[1].rstrip("/")}'
    return f"nrp:{parts[0]}"


def merge_raster_chunks(
    chunks_dir: str,
    output_dir: str,
    cleanup: bool = True,
    memory_limit: Optional[str] = None,
) -> int:
    """
    Consolidate ``chunks_dir/h0=*/part-*.parquet`` into ``output_dir/h0=*/data_0.parquet``.

    Args:
        chunks_dir: Where the sub-chunked hex step wrote its parts.
        output_dir: The published hex tree.
        cleanup: Remove the chunks prefix once the merge is verified.
        memory_limit: DuckDB memory limit (e.g. '8GiB'). Falls back to
            DUCKDB_MEMORY_LIMIT. Unset lets DuckDB auto-detect, which may
            ignore the container's cgroup limit.

    Returns:
        The number of h0 partitions written.
    """
    print(f"Merging raster hex chunks from {chunks_dir} to {output_dir}")

    con = ibis.duckdb.connect()
    configure_s3_credentials(con)
    con.raw_sql("SET preserve_insertion_order=false")
    effective_limit = memory_limit or os.environ.get("DUCKDB_MEMORY_LIMIT")
    if effective_limit:
        print(f"Setting DuckDB memory_limit={effective_limit}")
        con.raw_sql(f"SET memory_limit='{effective_limit}'")
    con.raw_sql("SET http_timeout=1200")
    con.raw_sql("SET http_retries=30")

    parts_glob = f"{chunks_dir.rstrip('/')}/h0=*/part-*.parquet"

    # hive_partitioning exposes the h0= directory as a column, so the partition
    # each part belongs to is read from the path rather than assumed from the
    # data — a part whose rows were filtered down to nothing still lands in the
    # right partition, and a mis-filed part is visible rather than silently
    # merged into whatever its first row's h0 happens to be.
    try:
        h0_vals = con.raw_sql(
            f"SELECT DISTINCT h0 FROM read_parquet('{parts_glob}', hive_partitioning=true) "
            "ORDER BY h0"
        ).fetchall()
    except Exception as e:
        raise RuntimeError(
            f"No parquet parts found under '{parts_glob}'. The sub-chunked hex "
            f"step may have produced no output, or wrote somewhere else."
        ) from e

    if not h0_vals:
        raise RuntimeError(
            f"No parquet parts found under '{parts_glob}'. The sub-chunked hex "
            f"step may have produced no output, or wrote somewhere else."
        )

    print(f"Merging {len(h0_vals)} h0 partitions one at a time (bounded memory)...")

    local_dir = "/tmp/hex-merge"
    os.makedirs(local_dir, exist_ok=True)
    rclone_output = _rclone_dest(output_dir)

    written = 0
    for (h0,) in h0_vals:
        local_partition = os.path.join(local_dir, f"h0={h0}")
        os.makedirs(local_partition, exist_ok=True)
        local_file = os.path.join(local_partition, "data_0.parquet")

        # EXCLUDE (h0) drops the hive column that read_parquet synthesised from
        # the directory name; the parts already carry their own h0 value column
        # when 0 is among --parent-resolutions, and re-adding the path-derived
        # one would either duplicate the column or change the output schema
        # relative to an unchunked build.
        con.raw_sql(
            f"COPY (SELECT * EXCLUDE (h0) FROM "
            f"read_parquet('{chunks_dir.rstrip('/')}/h0={h0}/part-*.parquet', "
            f"hive_partitioning=true)) "
            f"TO '{local_file}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

        if rclone_output:
            subprocess.run(
                ["rclone", "copy", local_partition, f"{rclone_output}/h0={h0}/",
                 "--transfers", "32",
                 "--s3-upload-concurrency", "16",
                 "--s3-chunk-size", "64M"],
                check=True,
            )
        else:
            dest = os.path.join(output_dir.rstrip("/"), f"h0={h0}")
            shutil.copytree(local_partition, dest, dirs_exist_ok=True)

        shutil.rmtree(local_partition)
        written += 1
        print(f"  h0={h0} done")

    # Verify through the consumer's own glob before deleting anything, so a
    # failure leaves the chunks intact to debug (issue #102's UBIGINT check).
    hex_glob = f"{output_dir.rstrip('/')}/h0=*/data_0.parquet"
    print(f"Asserting H3 columns are UBIGINT via {hex_glob}...")
    assert_h3_columns_unsigned(lambda sql: con.raw_sql(sql).fetchall(), hex_glob)

    shutil.rmtree(local_dir, ignore_errors=True)
    print(f"✓ Merged {written} h0 partitions")

    if cleanup and chunks_dir.startswith("s3://"):
        print("Removing chunks prefix from S3...")
        dest = _rclone_dest(chunks_dir)
        if dest:
            subprocess.run(["rclone", "purge", dest], check=False)

    return written
