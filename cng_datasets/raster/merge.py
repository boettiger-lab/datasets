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
from typing import List, Optional

import ibis
import yaml

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


def find_missing_chunks(chunks_dir: str, expect_chunks: int, con=None) -> List[int]:
    """Which chunk indices of a fan-out never recorded completion.

    **Enumerated, not counted.** A count answers "48 of 49" and leaves you to
    find the one, which on a 4,000-chunk fan-out is the whole problem: 3,779 of
    3,780 reads as complete at a glance. The markers carry their own index, so
    the missing set is exactly `range(expect_chunks)` minus what completed.

    Reads only the marker objects, so it needs no access to Armada or to
    Kubernetes — which is what lets it run as an ordinary S3 client after a job
    set has finished, however it was submitted.
    """
    own_con = con is None
    if own_con:
        con = ibis.duckdb.connect()
        configure_s3_credentials(con)
    try:
        manifest_glob = f"{chunks_dir.rstrip('/')}/_manifest/chunk-*.parquet"
        try:
            rows = con.raw_sql(
                f"SELECT DISTINCT chunk_index FROM read_parquet('{manifest_glob}')"
            ).fetchall()
        except Exception as e:
            raise RuntimeError(
                f"No completion markers found under '{manifest_glob}'. Either the hex "
                f"step did not run, or it predates completion markers — rerun the hex "
                f"step, or drop the expected-chunk count to proceed without the check."
            ) from e
        completed = {int(r[0]) for r in rows}
        unexpected = sorted(i for i in completed if i >= expect_chunks or i < 0)
        if unexpected:
            raise RuntimeError(
                f"Markers exist for chunk indices outside the expected range "
                f"0..{expect_chunks - 1}: {unexpected[:10]}. The chunks prefix was "
                f"written by a differently sized fan-out — chunk resolution or h0 "
                f"subset changed between the hex step and this one. Clear the prefix "
                f"and rerun rather than merging two fan-outs together."
            )
        return sorted(set(range(expect_chunks)) - completed)
    finally:
        if own_con:
            con.disconnect()


def _assert_all_chunks_ran(con, chunks_dir: str, expect_chunks: int) -> None:
    """Fail unless every chunk of the fan-out recorded that it completed.

    Counting part files cannot answer this: a chunk that does not overlap the
    raster legitimately writes none, so a missing part is indistinguishable from
    a chunk that never ran. Each chunk therefore leaves a marker whether or not
    it produced data, and this compares markers against the fan-out the
    generator sized.

    Without the check a partly failed fan-out merges its survivors into a
    complete-looking dataset and then, with cleanup on, deletes the evidence.
    That is most likely on the Armada backend, whose jobs carry no retry budget
    (issue #183) and which is where a large fan-out is routed (issue #173).
    """
    missing = find_missing_chunks(chunks_dir, expect_chunks, con=con)
    if missing:
        shown = ",".join(str(i) for i in missing[:20])
        more = f" (+{len(missing) - 20} more)" if len(missing) > 20 else ""
        raise RuntimeError(
            f"Incomplete fan-out: {len(missing)} of {expect_chunks} chunks never "
            f"recorded completion. Merging now would publish a short dataset and, "
            f"with cleanup on, delete the chunks that show which are missing.\n"
            f"  Missing chunk indices: {shown}{more}\n"
            f"  Rerun exactly those, then merge again — re-running a chunk is "
            f"idempotent, so a partial rerun is safe.\n"
            f"  `cng-datasets gapfill` emits a job set for precisely these indices. "
            f"On the Armada backend a failed job stays failed (no retry budget "
            f"survives conversion), so this is a normal pipeline stage rather than "
            f"an exception."
        )
    print(f"✓ All {expect_chunks} chunks recorded completion")


def merge_raster_chunks(
    chunks_dir: str,
    output_dir: str,
    cleanup: bool = True,
    memory_limit: Optional[str] = None,
    expect_chunks: Optional[int] = None,
) -> int:
    """
    Consolidate ``chunks_dir/h0=*/part-*.parquet`` into ``output_dir/h0=*/data_0.parquet``.

    Args:
        chunks_dir: Where the sub-chunked hex step wrote its parts.
        output_dir: The published hex tree.
        cleanup: Remove the chunks prefix once the merge is verified.
        expect_chunks: The number of chunks the hex fan-out was sized for. When
            given, the merge refuses to run unless that many chunks recorded
            completion.
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

    # Checked before anything is read or written, so an incomplete fan-out costs
    # nothing and leaves every chunk in place to inspect.
    if expect_chunks is not None:
        _assert_all_chunks_ran(con, chunks_dir, expect_chunks)

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


def generate_gapfill(
    chunks_dir: str,
    expect_chunks: int,
    hex_manifest: str,
    output_path: str,
    queue: Optional[str] = None,
    job_set_id: Optional[str] = None,
    priority_class: Optional[str] = None,
) -> List[int]:
    """
    Emit an Armada job set that re-runs exactly the chunks that never completed.

    Armada exposes no retry service on NRP — `armadactl get retry-policies`
    returns Unimplemented — and a preempted job is not rescheduled, so a
    transient fault leaves a permanently missing chunk. At an observed ~0.1%
    failure rate a few thousand units will lose one or two every run, which
    makes gap-fill a pipeline stage rather than an exception (issue #183).

    Gaps are found from the completion markers on S3, so this needs **no Armada
    and no Kubernetes access** — it runs as an ordinary S3 client after a job
    set has finished, whatever submitted it. Only re-submitting needs armadactl.

    The k8s backend does not need this: `backoffLimitPerIndex` retries a failed
    index in place, which is exactly the budget conversion cannot carry across.

    Args:
        chunks_dir: Where the hex step wrote its parts and markers.
        expect_chunks: Size of the fan-out a complete build has.
        hex_manifest: The generated `<name>-hex.yaml` the fan-out came from.
        output_path: Where to write the gap-fill job set.
        queue: Armada queue (default: the Job's namespace).
        job_set_id: Job set id (default: `<job name>-gapfill`).
        priority_class: Armada priority class for the re-run.

    Returns:
        The chunk indices the job set re-runs; empty when nothing is missing.
    """
    from cng_datasets.k8s.armada import k8s_indexed_job_to_armada, save_armada_yaml

    missing = find_missing_chunks(chunks_dir, expect_chunks)
    if not missing:
        print(f"✓ All {expect_chunks} chunks completed — nothing to gap-fill")
        return []

    with open(hex_manifest) as f:
        job_spec = yaml.safe_load(f)

    completions = job_spec.get("spec", {}).get("completions")
    if completions != expect_chunks:
        raise RuntimeError(
            f"{hex_manifest} declares {completions} completions but the expected "
            f"fan-out is {expect_chunks}. The manifest and the chunks prefix come "
            f"from different generations — re-running an index against the wrong "
            f"chunk list would process the wrong cell."
        )

    namespace = job_spec.get("metadata", {}).get("namespace", "default")
    name = job_spec.get("metadata", {}).get("name", "hex")
    armada_spec = k8s_indexed_job_to_armada(
        job_spec,
        queue=queue or namespace,
        job_set_id=job_set_id or f"{name}-gapfill",
        priority_class=priority_class,
        indices=missing,
    )
    save_armada_yaml(armada_spec, output_path)

    shown = ",".join(str(i) for i in missing[:20])
    more = f" (+{len(missing) - 20} more)" if len(missing) > 20 else ""
    print(f"\n⚠ {len(missing)} of {expect_chunks} chunks never completed")
    print(f"  Indices: {shown}{more}")
    print(f"  Wrote {output_path} — submit with:")
    print(f"    armadactl submit {output_path}")
    print("  Then rerun the merge; re-running a chunk is idempotent.")
    return missing
