"""Sizes and checksums of a dataset's outputs, for its STAC assets (issue #340)."""

import hashlib
import io
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from cng_datasets.storage.checksums import (
    compute_checksums,
    sha256_multihash,
    write_checksums,
)


def _multihash(data: bytes) -> str:
    return "1220" + hashlib.sha256(data).hexdigest()


class TestMultihash:
    def test_sha256_multihash_is_code_length_digest(self):
        digest = hashlib.sha256(b"abc").digest()
        mh = sha256_multihash(digest)
        raw = bytes.fromhex(mh)
        assert raw[0] == 0x12 and raw[1] == 32 and raw[2:] == digest
        assert mh == _multihash(b"abc")

    def test_a_digest_of_the_wrong_length_is_refused(self):
        with pytest.raises(ValueError):
            sha256_multihash(b"\x00" * 20)


@pytest.fixture
def tree(tmp_path):
    (tmp_path / "ds.parquet").write_bytes(b"parquet bytes")
    (tmp_path / "ds.pmtiles").write_bytes(b"")
    for cell, body in (("8001fffffffffff", b"a" * 10), ("8003fffffffffff", b"b" * 9_000_000)):
        part = tmp_path / "ds" / "hex" / f"h0={cell}"
        part.mkdir(parents=True)
        (part / "data_0.parquet").write_bytes(body)
    return tmp_path


class TestLocal:
    def test_files_and_a_prefix(self, tree):
        doc = compute_checksums([str(tree / "ds.parquet"), str(tree / "ds.pmtiles"),
                                 str(tree / "ds" / "hex") + "/"])
        assert "bucket" not in doc
        assert doc["generator"].startswith("cng-datasets ")
        files = doc["files"]
        assert len(files) == 4
        assert files[str(tree / "ds.parquet")]["file:size"] == 13
        assert files[str(tree / "ds.parquet")]["file:checksum"] == _multihash(b"parquet bytes")
        # An empty file still has a size and a checksum, of zero bytes.
        assert files[str(tree / "ds.pmtiles")] == {
            "file:size": 0, "file:checksum": _multihash(b""),
            "created": files[str(tree / "ds.pmtiles")]["created"]}
        # A file bigger than one read block hashes the same as all at once.
        big = str(tree / "ds" / "hex" / "h0=8003fffffffffff" / "data_0.parquet")
        assert files[big]["file:checksum"] == _multihash(b"b" * 9_000_000)

    def test_a_prefix_gets_a_total_and_a_count(self, tree):
        prefix = str(tree / "ds" / "hex") + "/"
        doc = compute_checksums([prefix])
        assert doc["prefixes"] == {prefix: {"file:size": 9_000_010, "file_count": 2}}

    def test_no_prefixes_key_without_a_prefix(self, tree):
        assert "prefixes" not in compute_checksums([str(tree / "ds.parquet")])

    def test_created_is_the_file_mtime_in_utc(self, tree):
        import os
        path = tree / "ds.parquet"
        os.utime(path, (1_700_000_000, 1_700_000_000))
        doc = compute_checksums([str(path)])
        assert doc["files"][str(path)]["created"] == "2023-11-14T22:13:20Z"

    def test_a_missing_file_fails(self, tree):
        with pytest.raises(ValueError, match="No such file"):
            compute_checksums([str(tree / "ds.parquet"), str(tree / "missing.pmtiles")])

    def test_an_empty_prefix_fails(self, tree):
        (tree / "empty").mkdir()
        with pytest.raises(ValueError, match="No files"):
            compute_checksums([str(tree / "empty") + "/"])

    def test_s3_and_local_cannot_mix(self, tree):
        with pytest.raises(ValueError, match="all s3"):
            compute_checksums(["s3://b/x.parquet", str(tree / "ds.parquet")])

    def test_the_output_inside_a_prefix_is_left_out(self, tree):
        out = tree / "ds" / "hex" / "checksums.json"
        out.write_text("{}")
        doc = write_checksums([str(tree / "ds" / "hex") + "/"], str(out))
        assert str(out) not in doc["files"]
        assert json.loads(out.read_text()) == doc

    def test_cli(self, tree, monkeypatch, capsys):
        from cng_datasets import cli
        out = tree / "ds.checksums.json"
        monkeypatch.setattr(sys, "argv", ["cng-datasets", "checksums", str(tree / "ds.parquet"),
                                          str(tree / "ds" / "hex") + "/", "--output", str(out)])
        cli.main()
        doc = json.loads(out.read_text())
        assert len(doc["files"]) == 3
        assert "3 files" in capsys.readouterr().out


class FakeS3:
    """The four boto3 calls the checksums step makes, over a dict of objects."""

    def __init__(self, bucket, objects):
        self.bucket = bucket
        self.objects = objects
        self.when = datetime(2026, 11, 18, 19, 40, 12, tzinfo=timezone.utc)
        self.put = {}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        fake = self

        class Paginator:
            def paginate(self, Bucket, Prefix):
                assert Bucket == fake.bucket
                keys = sorted(k for k in fake.objects if k.startswith(Prefix))
                # Two pages, as a long listing would come back.
                half = len(keys) // 2
                for page in (keys[:half], keys[half:]):
                    yield {"Contents": [{"Key": k, "LastModified": fake.when} for k in page]}
        return Paginator()

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"LastModified": self.when}

    def get_object(self, Bucket, Key):
        data = self.objects[Key]

        class Body(io.BytesIO):
            def iter_chunks(self, size):
                while True:
                    block = self.read(size)
                    if not block:
                        return
                    yield block
        return {"Body": Body(data)}

    def put_object(self, Bucket, Key, Body, ContentType):
        self.put[(Bucket, Key)] = (Body, ContentType)


class TestS3:
    @pytest.fixture
    def s3(self):
        return FakeS3("public-x", {
            "a/ds.parquet": b"pq",
            "a/ds.pmtiles": b"pm",
            "a/ds/hex/h0=1/data_0.parquet": b"h1",
            "a/ds/hex/h0=2/data_0.parquet": b"h22",
            "a/ds-other.parquet": b"not ours",
        })

    def test_keys_are_relative_to_the_bucket(self, s3):
        doc = write_checksums(
            ["s3://public-x/a/ds.parquet", "s3://public-x/a/ds.pmtiles", "s3://public-x/a/ds/hex/"],
            "s3://public-x/a/ds.checksums.json", client=s3)
        assert doc["bucket"] == "public-x"
        assert list(doc["files"]) == ["a/ds.parquet", "a/ds.pmtiles",
                                      "a/ds/hex/h0=1/data_0.parquet", "a/ds/hex/h0=2/data_0.parquet"]
        assert doc["files"]["a/ds.parquet"] == {
            "file:size": 2, "file:checksum": _multihash(b"pq"), "created": "2026-11-18T19:40:12Z"}
        assert doc["prefixes"] == {"a/ds/hex/": {"file:size": 5, "file_count": 2}}
        body, ctype = s3.put[("public-x", "a/ds.checksums.json")]
        assert ctype == "application/json" and json.loads(body) == doc

    def test_a_missing_object_fails(self, s3):
        with pytest.raises(ValueError, match="Cannot read"):
            compute_checksums(["s3://public-x/a/ds.pmtiles", "s3://public-x/a/gone.pmtiles"],
                              client=s3)

    def test_one_bucket_only(self, s3):
        with pytest.raises(ValueError, match="several buckets"):
            compute_checksums(["s3://public-x/a/ds.parquet", "s3://other/a.parquet"], client=s3)

    def test_the_output_goes_in_the_files_bucket(self, s3):
        with pytest.raises(ValueError, match="not the files' bucket"):
            write_checksums(["s3://public-x/a/ds.parquet"], "s3://other/ds.checksums.json",
                            client=s3)

    def test_endpoint_from_the_job_environment(self, monkeypatch):
        from cng_datasets.storage.checksums import s3_client
        monkeypatch.setenv("AWS_S3_ENDPOINT", "rook-ceph-rgw-nautiluss3.rook")
        monkeypatch.setenv("AWS_HTTPS", "false")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "k")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
        assert s3_client().meta.endpoint_url == "http://rook-ceph-rgw-nautiluss3.rook"


def _script(tmpdir):
    wf = yaml.safe_load((Path(tmpdir) / "workflow.yaml").read_text())
    return wf["spec"]["template"]["spec"]["containers"][0]["args"][0]


def _command(tmpdir, name):
    job = yaml.safe_load((Path(tmpdir) / f"{name}-checksums.yaml").read_text())
    container = job["spec"]["template"]["spec"]["containers"][0]
    assert "priorityClassName" not in job["spec"]["template"]["spec"]
    return container["command"][2]


class TestWorkflowStep:
    @pytest.mark.timeout(10)
    def test_vector_workflow_checksums_every_output_last(self, monkeypatch):
        from cng_datasets.k8s.workflows import generate_dataset_workflow
        import cng_datasets.k8s.workflows as wf
        monkeypatch.setattr(wf, "_count_source_features", lambda *a, **k: 5000)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(dataset_name="census/county", source_urls="https://e.com/x.gpkg",
                                      bucket="public-census", output_dir=tmpdir, h3_resolution=8)
            cmd = _command(tmpdir, "census-county")
            script = _script(tmpdir)
            cm = yaml.safe_load((Path(tmpdir) / "configmap.yaml").read_text())
        assert ("cng-datasets checksums s3://public-census/census/county.parquet "
                "s3://public-census/census/county.pmtiles s3://public-census/census/county/hex/ "
                "--output s3://public-census/census/county.checksums.json") in cmd
        apply = script.index("-f /yamls/census-county-checksums.yaml")
        for step in ("repartition", "pmtiles"):
            assert script.index(f"wait_job census-county-{step} ") < apply
        assert "census-county-checksums.yaml" in cm["data"]

    @pytest.mark.timeout(10)
    def test_chunked_pmtiles_are_merged_before_the_checksums(self, monkeypatch):
        from cng_datasets.k8s.workflows import generate_dataset_workflow
        import cng_datasets.k8s.workflows as wf
        monkeypatch.setattr(wf, "_count_source_features", lambda *a, **k: 5000)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(dataset_name="nwi", source_urls="https://e.com/x.gpkg",
                                      bucket="b", output_dir=tmpdir, h3_resolution=8,
                                      pmtiles_chunks=4)
            script = _script(tmpdir)
        assert script.index("wait_job nwi-pmtiles-merge ") < script.index("-f /yamls/nwi-checksums.yaml")

    @pytest.mark.timeout(10)
    def test_raster_lists_the_cogs_it_wrote_per_year(self, monkeypatch):
        from cng_datasets.k8s.workflows import generate_raster_workflow
        import cng_datasets.raster.cog as cog
        monkeypatch.setattr(cog, "is_cog", lambda *a, **k: False)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(dataset_name="lc/nlcd", source_urls="s3://src/n_{year}.tif",
                                     bucket="public-lc", output_dir=tmpdir, years="2000-2001")
            cmd = _command(tmpdir, "lc-nlcd")
            script = _script(tmpdir)
        assert ("checksums s3://public-lc/lc/nlcd-2000-cog.tif s3://public-lc/lc/nlcd-2001-cog.tif "
                "s3://public-lc/lc/nlcd/hex/ --output s3://public-lc/lc/nlcd.checksums.json") in cmd
        assert script.index("wait_job lc-nlcd-hex ") < script.index("-f /yamls/lc-nlcd-checksums.yaml")

    @pytest.mark.timeout(10)
    def test_a_source_cog_is_not_an_output(self, monkeypatch):
        from cng_datasets.k8s.workflows import generate_raster_workflow
        import cng_datasets.raster.cog as cog
        monkeypatch.setattr(cog, "is_cog", lambda *a, **k: True)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(dataset_name="ras", source_urls="https://e.com/x.tif",
                                     bucket="b", output_dir=tmpdir, chunk_resolution=1)
            cmd = _command(tmpdir, "ras")
            script = _script(tmpdir)
        assert "checksums s3://b/ras/hex/ --output s3://b/ras.checksums.json" in cmd
        assert script.index("wait_job ras-merge ") < script.index("-f /yamls/ras-checksums.yaml")


class TestEveryAppliedStepIsShipped:
    """The orchestrator applies each step from the ConfigMap, so a step it
    applies but the generator did not write fails the build at that step."""

    @staticmethod
    def _check(tmpdir):
        import re
        shipped = set(yaml.safe_load((Path(tmpdir) / "configmap.yaml").read_text())["data"])
        applied = re.findall(r"-f /yamls/(\S+\.yaml)", _script(tmpdir))
        assert applied
        for name in applied:
            assert name in shipped and (Path(tmpdir) / name).exists(), name

    @pytest.mark.timeout(10)
    @pytest.mark.parametrize("chunks", [None, 4])
    def test_vector(self, monkeypatch, chunks):
        from cng_datasets.k8s.workflows import generate_dataset_workflow
        import cng_datasets.k8s.workflows as wf
        monkeypatch.setattr(wf, "_count_source_features", lambda *a, **k: 5000)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(dataset_name="vec", source_urls="https://e.com/x.gpkg",
                                      bucket="b", output_dir=tmpdir, h3_resolution=8,
                                      pmtiles_chunks=chunks)
            self._check(tmpdir)

    @pytest.mark.timeout(10)
    @pytest.mark.parametrize("is_cog,chunk_resolution", [(True, 0), (False, 0), (True, 1)])
    def test_raster(self, monkeypatch, is_cog, chunk_resolution):
        from cng_datasets.k8s.workflows import generate_raster_workflow
        import cng_datasets.raster.cog as cog
        monkeypatch.setattr(cog, "is_cog", lambda *a, **k: is_cog)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(dataset_name="ras", source_urls="https://e.com/x.tif",
                                     bucket="b", output_dir=tmpdir,
                                     chunk_resolution=chunk_resolution)
            self._check(tmpdir)
