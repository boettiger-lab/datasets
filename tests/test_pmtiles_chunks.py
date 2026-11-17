"""Tests for the chunked PMTiles backend (issue #83).

The workflow tests read the generated manifests. The tippecanoe tests build
real archives and are skipped where tippecanoe is not installed (it is in the
project image, where CI runs).
"""

import gzip
import json
import shutil
import struct
import subprocess
import tempfile
from pathlib import Path

import pytest
import yaml

from cng_datasets.k8s import generate_dataset_workflow
from cng_datasets.vector.pmtiles import (
    TILE_SIZE_LIMIT,
    iter_tile_sizes,
    oversized_tiles,
    tile_zoom,
)
from cng_datasets.vector.pmtiles_chunks import (
    DEFAULT_SPLIT_ZOOM,
    chunk_bounds,
    overview_tolerance,
)

needs_tippecanoe = pytest.mark.skipif(
    not (shutil.which("tippecanoe") and shutil.which("tile-join")),
    reason="tippecanoe and tile-join are in the project image, not installed here")


# --- Row ranges --------------------------------------------------------------

class TestChunkBounds:
    @pytest.mark.parametrize("n_rows,n_chunks", [(10, 3), (1000, 7), (5, 5), (3, 8), (0, 2)])
    def test_ranges_cover_every_row_once(self, n_rows, n_chunks):
        ranges = [chunk_bounds(n_rows, n_chunks, i) for i in range(n_chunks)]
        assert ranges[0][0] == 0 and ranges[-1][1] == n_rows
        for (_, stop), (start, _) in zip(ranges, ranges[1:]):
            assert stop == start
        sizes = [stop - start for start, stop in ranges]
        assert max(sizes) - min(sizes) <= 1

    def test_more_chunks_than_rows_leaves_some_empty(self):
        ranges = [chunk_bounds(3, 8, i) for i in range(8)]
        assert sum(1 for a, b in ranges if b > a) == 3

    @pytest.mark.parametrize("n_chunks,index", [(0, 0), (4, 4), (4, -1)])
    def test_bad_arguments_are_refused(self, n_chunks, index):
        with pytest.raises(ValueError):
            chunk_bounds(10, n_chunks, index)


def test_overview_tolerance_is_half_a_pixel_at_the_overviews_top_zoom():
    # z6 is the overview's top zoom when the parts start at z7.
    assert overview_tolerance(7) == pytest.approx(360 / 2 ** 6 / 4096 / 2)
    assert overview_tolerance(8) == pytest.approx(overview_tolerance(7) / 2)
    with pytest.raises(ValueError):
        overview_tolerance(0)


# --- Reading tile sizes from a PMTiles directory -----------------------------

def _varint(n):
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _directory(entries):
    """Encode (tile_id, run_length, offset, length) entries as a PMTiles directory."""
    buf = _varint(len(entries))
    last = 0
    for tile_id, *_ in entries:
        buf += _varint(tile_id - last)
        last = tile_id
    for _, run_length, _, _ in entries:
        buf += _varint(run_length)
    for _, _, _, length in entries:
        buf += _varint(length)
    for i, (_, _, offset, length) in enumerate(entries):
        prev = entries[i - 1] if i else None
        contiguous = prev is not None and offset == prev[2] + prev[3]
        buf += _varint(0 if contiguous else offset + 1)
    return gzip.compress(buf)


def _archive(root_entries, leaf_entries):
    """A PMTiles v3 archive holding only a header, a root and one leaf directory."""
    leaf = _directory(leaf_entries)
    root = _directory([e if e[1] else (e[0], 0, 0, len(leaf)) for e in root_entries])
    header = bytearray(127)
    header[0:7] = b"PMTiles"
    header[7] = 3
    struct.pack_into("<QQ", header, 8, 127, len(root))
    struct.pack_into("<Q", header, 40, 127 + len(root))   # leaf directories
    header[97] = 2                                        # gzip
    return bytes(header) + root + leaf


class TestTileSizes:
    def test_tile_zoom(self):
        assert [tile_zoom(t) for t in (0, 1, 4, 5, 20, 21, 84, 85)] == [0, 1, 1, 2, 2, 3, 3, 4]

    def test_walks_root_and_leaf_directories(self, tmp_path):
        big = TILE_SIZE_LIMIT + 1
        root = [(0, 1, 0, 100), (1, 2, 100, big), (5, 0, None, None)]   # last: a leaf
        leaf = [(5, 1, 0, 10), (21, 1, 10, big + 5), (22, 3, 600_000, 50)]
        path = tmp_path / "t.pmtiles"
        path.write_bytes(_archive(root, leaf))

        tiles = sorted(iter_tile_sizes(str(path)))
        assert tiles == [(0, 1, 100), (1, 2, big), (5, 1, 10), (21, 1, big + 5), (22, 3, 50)]

        report = oversized_tiles(str(path))
        assert report["tiles"] == 1 + 2 + 1 + 1 + 3
        assert report["oversized"] == 3          # the run of two at z1, one at z3
        assert report["by_zoom"] == {1: 2, 3: 1}
        assert report["largest"] == (21, 3, big + 5)

    def test_not_a_pmtiles_archive(self, tmp_path):
        path = tmp_path / "x.pmtiles"
        path.write_bytes(b"\0" * 127)
        with pytest.raises(ValueError):
            list(iter_tile_sizes(str(path)))


def _points_geojsonl(path, lon0, lon1, n=4000):
    with open(path, "w") as f:
        for i in range(n):
            lon = lon0 + (lon1 - lon0) * (i % 100) / 100
            lat = 30 + 15 * (i // 100) / (n / 100)
            f.write(json.dumps({"type": "Feature", "properties": {"i": i},
                                "geometry": {"type": "Point", "coordinates": [lon, lat]}}) + "\n")


def _tippecanoe(out, src, *flags):
    subprocess.run(["tippecanoe", "-q", "-o", str(out), "-l", "d", "-r1", "--force", *flags, str(src)],
                   check=True)


@needs_tippecanoe
class TestRealArchives:
    def test_every_addressed_tile_is_found(self, tmp_path):
        src = tmp_path / "p.geojsonl"
        _points_geojsonl(src, -120, -80)
        out = tmp_path / "p.pmtiles"
        _tippecanoe(out, src, "-z", "9")
        addressed = struct.unpack_from("<Q", out.read_bytes(), 72)[0]
        assert oversized_tiles(str(out))["tiles"] == addressed > 0

    def test_overview_and_parts_join_into_every_zoom(self, tmp_path):
        """The merge's shape: overview z0..z(K-1) and parts zK..zmax, tile-join -pk."""
        west, east = tmp_path / "w.geojsonl", tmp_path / "e.geojsonl"
        _points_geojsonl(west, -120, -100)
        _points_geojsonl(east, -100, -80)
        whole = tmp_path / "all.geojsonl"
        whole.write_text(west.read_text() + east.read_text())
        _tippecanoe(tmp_path / "overview.pmtiles", whole, "-z", "4")
        _tippecanoe(tmp_path / "w.pmtiles", west, "-Z", "5", "-z", "8")
        _tippecanoe(tmp_path / "e.pmtiles", east, "-Z", "5", "-z", "8")
        merged = tmp_path / "m.pmtiles"
        subprocess.run(["tile-join", "-q", "-pk", "--force", "-o", str(merged),
                        str(tmp_path / "overview.pmtiles"), str(tmp_path / "w.pmtiles"),
                        str(tmp_path / "e.pmtiles")], check=True)
        zooms = {tile_zoom(t) for t, _, _ in iter_tile_sizes(str(merged))}
        assert zooms == set(range(0, 9))


# --- Extracting a row range --------------------------------------------------

@pytest.fixture
def lines_parquet(tmp_path):
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    path = tmp_path / "src.parquet"
    # 30 wiggly lines with many vertices, numbered in file order.
    con.execute(f"""COPY (
        SELECT i AS _cng_fid,
               ST_GeomFromText('LINESTRING(' || (
                   SELECT string_agg((i + j / 1000.0)::VARCHAR || ' ' || (sin(j) / 1e4)::VARCHAR, ',')
                   FROM range(200) t(j)) || ')') AS geom
        FROM range(30) s(i) ORDER BY i) TO '{path}' (FORMAT PARQUET, ROW_GROUP_SIZE 4)""")
    return path


class TestExtractChunk:
    def _read(self, path):
        import duckdb
        con = duckdb.connect()
        con.execute("LOAD spatial")
        return con.execute(f"SELECT _cng_fid, ST_NPoints(geom) FROM '{path}'").fetchall()

    def test_writes_its_own_rows_in_file_order(self, lines_parquet, tmp_path):
        from cng_datasets.vector.pmtiles_chunks import extract_chunk
        out = tmp_path / "c.parquet"
        assert extract_chunk(str(lines_parquet), str(out), n_chunks=3, index=1) == 10
        import duckdb
        # Rows 10..19 of the file, in the file's own order.
        expected = [r[0] for r in duckdb.sql(
            f"SELECT _cng_fid FROM read_parquet('{lines_parquet}', file_row_number = true) "
            f"WHERE file_row_number BETWEEN 10 AND 19 ORDER BY file_row_number").fetchall()]
        rows = self._read(out)
        assert [r[0] for r in rows] == expected
        assert all(n == 200 for _, n in rows)

    def test_simplify_drops_vertices_but_keeps_features(self, lines_parquet, tmp_path):
        from cng_datasets.vector.pmtiles_chunks import extract_chunk
        out = tmp_path / "o.parquet"
        assert extract_chunk(str(lines_parquet), str(out), simplify=0.01) == 30
        rows = self._read(out)
        assert len(rows) == 30 and all(n < 200 for _, n in rows)

    def test_an_empty_range_writes_nothing(self, lines_parquet, tmp_path):
        from cng_datasets.vector.pmtiles_chunks import extract_chunk
        out = tmp_path / "e.parquet"
        # 30 rows in 40 chunks: index 0 is rows 0..0, an empty range.
        assert extract_chunk(str(lines_parquet), str(out), n_chunks=40, index=0) == 0
        assert not out.exists()


# --- Workflow generation -----------------------------------------------------

@pytest.fixture
def offline(monkeypatch):
    import cng_datasets.k8s.workflows as wf
    monkeypatch.setattr(wf, "_count_source_features", lambda *a, **k: 38_000_000)
    monkeypatch.setattr(wf, "_detect_geometry_type", lambda *a, **k: "polygon")


def _generate(tmpdir, **kwargs):
    generate_dataset_workflow(dataset_name="wet/nwi", source_url="https://example.com/x.gpkg",
                              bucket="b", output_dir=tmpdir, **kwargs)
    out = {}
    for path in Path(tmpdir).glob("*.yaml"):
        out[path.name] = next(yaml.safe_load_all(path.read_text()))
    return out


def _script(job):
    return job["spec"]["template"]["spec"]["containers"][0]["command"][-1]


def _workflow_script(files):
    return files["workflow.yaml"]["spec"]["template"]["spec"]["containers"][0]["args"][0]


@pytest.mark.usefixtures("offline")
class TestChunkedWorkflow:
    def test_single_pod_stays_the_default(self, tmp_path):
        files = _generate(tmp_path)
        assert "wet-nwi-pmtiles.yaml" in files
        assert not any("pmtiles-parts" in name for name in files)

    def test_three_jobs_replace_the_single_pod(self, tmp_path):
        files = _generate(tmp_path, pmtiles_chunks=60, max_parallelism=50)
        assert "wet-nwi-pmtiles.yaml" not in files
        parts = files["wet-nwi-pmtiles-parts.yaml"]
        assert parts["spec"]["completionMode"] == "Indexed"
        assert parts["spec"]["completions"] == 60
        assert parts["spec"]["parallelism"] == 50
        assert "--chunks 60 --index $JOB_COMPLETION_INDEX" in _script(parts)
        assert "wet-nwi-pmtiles-overview.yaml" in files
        assert "wet-nwi-pmtiles-merge.yaml" in files

    def test_parts_build_high_zooms_and_overview_the_rest(self, tmp_path):
        files = _generate(tmp_path, pmtiles_chunks=8, h3_resolution=10)
        part_line = next(ln for ln in _script(files["wet-nwi-pmtiles-parts.yaml"]).splitlines()
                         if ln.startswith("tippecanoe "))
        assert f"-Z {DEFAULT_SPLIT_ZOOM} -z 13" in part_line
        assert " -r1 " in part_line and "--drop-densest-as-needed" in part_line
        overview = _script(files["wet-nwi-pmtiles-overview.yaml"])
        assert f"-z {DEFAULT_SPLIT_ZOOM - 1}" in overview
        assert f"--simplify {overview_tolerance(DEFAULT_SPLIT_ZOOM)!r}" in overview
        assert "set -eo pipefail" in overview   # ogr2ogr | tippecanoe must not hide a failure

    def test_split_zoom_is_honoured_and_checked(self, tmp_path):
        files = _generate(tmp_path, pmtiles_chunks=8, pmtiles_split_zoom=9)
        assert "-Z 9 -z 13" in _script(files["wet-nwi-pmtiles-parts.yaml"])
        with pytest.raises(ValueError, match="split-zoom"):
            _generate(tmp_path / "a", pmtiles_chunks=8, pmtiles_split_zoom=14)
        with pytest.raises(ValueError, match="only with --pmtiles-chunks"):
            _generate(tmp_path / "b", pmtiles_split_zoom=9)

    def test_too_many_chunks_is_refused(self, tmp_path):
        with pytest.raises(ValueError, match="max-completions"):
            _generate(tmp_path, pmtiles_chunks=201)

    def test_merge_keeps_oversized_tiles_and_reports_them(self, tmp_path):
        """Without -pk, tile-join drops a merged tile over 500 KB and exits 0 (#83)."""
        merge = _script(_generate(tmp_path, pmtiles_chunks=4)["wet-nwi-pmtiles-merge.yaml"])
        join = next(ln for ln in merge.splitlines() if ln.startswith("tile-join "))
        assert " -pk " in join
        assert merge.index("tile-join ") < merge.index("cng-datasets pmtiles-check")
        assert merge.index("pmtiles-check") < merge.index("rclone purge")

    def test_merge_refuses_a_missing_part(self, tmp_path):
        merge = _script(_generate(tmp_path, pmtiles_chunks=4)["wet-nwi-pmtiles-merge.yaml"])
        assert "seq 0 3" in merge
        assert "not merging" in merge and "exit 1" in merge
        # Joined from the expected indexes, never a glob of whatever is there.
        assert "part-*" not in merge

    def test_empty_part_leaves_a_marker(self, tmp_path):
        parts = _script(_generate(tmp_path, pmtiles_chunks=4)["wet-nwi-pmtiles-parts.yaml"])
        assert ".empty" in parts and "exit 0" in parts

    def test_outputs_land_where_the_single_pod_puts_them(self, tmp_path):
        files = _generate(tmp_path, pmtiles_chunks=4)
        merge = _script(files["wet-nwi-pmtiles-merge.yaml"])
        assert "rclone copy /tmp/$DATASET.pmtiles nrp:b/wet/" in merge
        env = {e["name"]: e.get("value") for e in
               files["wet-nwi-pmtiles-merge.yaml"]["spec"]["template"]["spec"]["containers"][0]["env"]}
        assert env["DATASET"] == "nwi"
        assert "nrp:b/wet/nwi/pmtiles-parts" in _script(files["wet-nwi-pmtiles-parts.yaml"])

    def test_orchestrator_merges_after_both_inputs_finish(self, tmp_path):
        script = _workflow_script(_generate(tmp_path, pmtiles_chunks=4))
        apply_parts = script.index("-pmtiles-parts.yaml")
        apply_overview = script.index("-pmtiles-overview.yaml")
        apply_hex = script.index("-hex.yaml")
        assert apply_parts < apply_hex and apply_overview < apply_hex
        merge = script.index("-pmtiles-merge.yaml")
        assert script.index("wait_job wet-nwi-pmtiles-parts") < merge
        assert script.index("wait_job wet-nwi-pmtiles-overview") < merge
        assert "wait_job wet-nwi-pmtiles-merge" in script
        assert "may still be running in the background" not in script

    def test_configmap_carries_the_chunked_jobs(self, tmp_path):
        data = _generate(tmp_path, pmtiles_chunks=4)["configmap.yaml"]["data"]
        for suffix in ("parts", "overview", "merge"):
            assert f"wet-nwi-pmtiles-{suffix}.yaml" in data
        assert "wet-nwi-pmtiles.yaml" not in data

    def test_pods_stay_within_the_house_limits(self, tmp_path):
        files = _generate(tmp_path, pmtiles_chunks=200, max_parallelism=50)
        for suffix in ("parts", "overview", "merge"):
            job = files[f"wet-nwi-pmtiles-{suffix}.yaml"]
            res = job["spec"]["template"]["spec"]["containers"][0]["resources"]
            assert res["requests"]["ephemeral-storage"] == "50Gi"
            assert job["spec"].get("parallelism", 1) <= 200
            assert "priorityClassName" not in job["spec"]["template"]["spec"]
