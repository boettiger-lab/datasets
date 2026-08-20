"""
Unit tests for Armada job generation.
"""

import tempfile
import yaml
from pathlib import Path
import pytest

from cng_datasets.k8s.armada import (
    k8s_job_to_armada,
    k8s_indexed_job_to_armada,
    convert_workflow_to_armada,
    save_armada_yaml,
    resolve_armada_priority_class,
    DEFAULT_ARMADA_PRIORITY_CLASS,
    _extract_pod_spec,
    _replace_completion_index,
)
from cng_datasets.k8s import generate_dataset_workflow
from cng_datasets.k8s.workflows import generate_raster_workflow


def _make_simple_job(name="test-job", namespace="biodiversity"):
    """Create a minimal k8s Job spec for testing."""
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "completions": 1,
            "parallelism": 1,
            "template": {
                "spec": {
                    "priorityClassName": "opportunistic",
                    "restartPolicy": "Never",
                    "containers": [{
                        "name": "worker",
                        "image": "alpine:latest",
                        "command": ["echo", "hello"],
                        "resources": {
                            "requests": {"cpu": "1", "memory": "2Gi"},
                            "limits": {"cpu": "1", "memory": "2Gi"},
                        },
                    }],
                }
            },
        },
    }


def _make_indexed_job(name="hex-job", namespace="biodiversity", completions=5):
    """Create a k8s Indexed Job spec for testing."""
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "completions": completions,
            "parallelism": 3,
            "completionMode": "Indexed",
            "template": {
                "spec": {
                    "priorityClassName": "opportunistic",
                    "restartPolicy": "Never",
                    "containers": [{
                        "name": "hex-task",
                        "image": "ghcr.io/boettiger-lab/datasets:latest",
                        "command": ["bash", "-c", "cng-datasets vector --chunk-id ${JOB_COMPLETION_INDEX} --chunk-size 100"],
                        "resources": {
                            "requests": {"cpu": "4", "memory": "8Gi"},
                            "limits": {"cpu": "4", "memory": "8Gi"},
                        },
                    }],
                }
            },
        },
    }


class TestExtractPodSpec:
    @pytest.mark.timeout(5)
    def test_extracts_pod_spec(self):
        job = _make_simple_job()
        pod = _extract_pod_spec(job)
        assert "containers" in pod
        assert pod["containers"][0]["image"] == "alpine:latest"

    @pytest.mark.timeout(5)
    def test_strips_priority_class(self):
        job = _make_simple_job()
        pod = _extract_pod_spec(job)
        assert "priorityClassName" not in pod

    @pytest.mark.timeout(5)
    def test_does_not_mutate_original(self):
        job = _make_simple_job()
        _extract_pod_spec(job)
        assert job["spec"]["template"]["spec"]["priorityClassName"] == "opportunistic"


class TestReplaceCompletionIndex:
    @pytest.mark.timeout(5)
    def test_replaces_in_command(self):
        pod = {
            "containers": [{
                "command": ["bash", "-c", "echo ${JOB_COMPLETION_INDEX}"],
            }]
        }
        _replace_completion_index(pod, 42)
        assert pod["containers"][0]["command"][2] == "echo 42"

    @pytest.mark.timeout(5)
    def test_replaces_in_args(self):
        pod = {
            "containers": [{
                "args": ["--index", "${JOB_COMPLETION_INDEX}"],
            }]
        }
        _replace_completion_index(pod, 7)
        assert pod["containers"][0]["args"][1] == "7"

    @pytest.mark.timeout(5)
    def test_replaces_without_braces(self):
        pod = {
            "containers": [{
                "command": ["bash", "-c", "echo $JOB_COMPLETION_INDEX"],
            }]
        }
        _replace_completion_index(pod, 0)
        assert pod["containers"][0]["command"][2] == "echo 0"

    @pytest.mark.timeout(5)
    def test_replaces_embedded_in_string(self):
        pod = {
            "containers": [{
                "command": ["bash", "-c", "cmd --chunk-id ${JOB_COMPLETION_INDEX} --size 100"],
            }]
        }
        _replace_completion_index(pod, 99)
        assert "99" in pod["containers"][0]["command"][2]
        assert "${JOB_COMPLETION_INDEX}" not in pod["containers"][0]["command"][2]


class TestK8sJobToArmada:
    @pytest.mark.timeout(5)
    def test_basic_conversion(self):
        job = _make_simple_job()
        result = k8s_job_to_armada(job, queue="biodiversity", job_set_id="test-set")

        assert result["queue"] == "biodiversity"
        assert result["jobSetId"] == "test-set"
        assert len(result["jobs"]) == 1

    @pytest.mark.timeout(5)
    def test_armada_job_structure(self):
        job = _make_simple_job(namespace="my-ns")
        result = k8s_job_to_armada(job, queue="my-ns", job_set_id="test")

        armada_job = result["jobs"][0]
        assert armada_job["namespace"] == "my-ns"
        assert armada_job["priorityClassName"] == "armada-default"
        assert "podSpec" in armada_job
        assert armada_job["podSpec"]["containers"][0]["image"] == "alpine:latest"

    @pytest.mark.timeout(5)
    def test_opportunistic_does_not_become_preemptible(self):
        """
        A k8s 'opportunistic' job must not convert to armada-preemptible.

        Opportunistic pods are preempted but recreated by their Job controller;
        preempted Armada jobs are not rescheduled at all, and the Job-level
        retry settings do not survive conversion. The default is therefore
        non-preemptible (issue #183).
        """
        job = _make_simple_job()
        assert job["spec"]["template"]["spec"]["priorityClassName"] == "opportunistic"
        result = k8s_job_to_armada(job, queue="q", job_set_id="s")
        assert result["jobs"][0]["priorityClassName"] == "armada-default"
        assert result["jobs"][0]["priorityClassName"] == DEFAULT_ARMADA_PRIORITY_CLASS

    @pytest.mark.timeout(5)
    def test_custom_priority_class(self):
        job = _make_simple_job()
        result = k8s_job_to_armada(
            job, queue="q", job_set_id="s", priority_class="armada-default"
        )
        assert result["jobs"][0]["priorityClassName"] == "armada-default"

    @pytest.mark.timeout(5)
    def test_priority_class_shorthand(self):
        job = _make_simple_job()
        result = k8s_job_to_armada(
            job, queue="q", job_set_id="s", priority_class="preemptible"
        )
        assert result["jobs"][0]["priorityClassName"] == "armada-preemptible"

    @pytest.mark.timeout(5)
    def test_pod_spec_has_no_priority_class(self):
        job = _make_simple_job()
        result = k8s_job_to_armada(job, queue="q", job_set_id="s")
        assert "priorityClassName" not in result["jobs"][0]["podSpec"]


class TestK8sIndexedJobToArmada:
    @pytest.mark.timeout(5)
    def test_creates_n_jobs(self):
        job = _make_indexed_job(completions=5)
        result = k8s_indexed_job_to_armada(job, queue="q", job_set_id="hex")
        assert len(result["jobs"]) == 5

    @pytest.mark.timeout(5)
    def test_each_job_has_correct_index(self):
        job = _make_indexed_job(completions=3)
        result = k8s_indexed_job_to_armada(job, queue="q", job_set_id="hex")

        for i, armada_job in enumerate(result["jobs"]):
            cmd = armada_job["podSpec"]["containers"][0]["command"][2]
            assert f"--chunk-id {i}" in cmd

    @pytest.mark.timeout(5)
    def test_no_completion_index_references_remain(self):
        job = _make_indexed_job(completions=3)
        result = k8s_indexed_job_to_armada(job, queue="q", job_set_id="hex")

        for armada_job in result["jobs"]:
            full_text = str(armada_job)
            assert "JOB_COMPLETION_INDEX" not in full_text

    @pytest.mark.timeout(5)
    def test_jobs_are_independent_copies(self):
        """Ensure modifying one job doesn't affect others."""
        job = _make_indexed_job(completions=2)
        result = k8s_indexed_job_to_armada(job, queue="q", job_set_id="hex")

        result["jobs"][0]["podSpec"]["containers"][0]["image"] = "modified"
        assert result["jobs"][1]["podSpec"]["containers"][0]["image"] != "modified"

    @pytest.mark.timeout(5)
    def test_shared_metadata(self):
        job = _make_indexed_job(completions=2, namespace="bio")
        result = k8s_indexed_job_to_armada(job, queue="bio", job_set_id="my-hex")

        assert result["queue"] == "bio"
        assert result["jobSetId"] == "my-hex"
        for armada_job in result["jobs"]:
            assert armada_job["namespace"] == "bio"


class TestResolveArmadaPriorityClass:
    """Shorthand resolution for --armada-priority-class (issue #183)."""

    @pytest.mark.timeout(5)
    def test_shorthands(self):
        assert resolve_armada_priority_class("default") == "armada-default"
        assert resolve_armada_priority_class("preemptible") == "armada-preemptible"
        assert resolve_armada_priority_class("high") == "armada-high-priority"

    @pytest.mark.timeout(5)
    def test_literal_class_passes_through(self):
        """Cluster-specific class names must stay usable."""
        assert resolve_armada_priority_class("armada-default") == "armada-default"
        assert resolve_armada_priority_class("some-other-class") == "some-other-class"

    @pytest.mark.timeout(5)
    def test_unset_returns_none(self):
        assert resolve_armada_priority_class(None) is None
        assert resolve_armada_priority_class("") is None


class TestDroppedRetrySettingsWarning:
    """
    Conversion takes spec.template.spec, so Job-level retry settings are left
    behind. That must be announced, not silent (issue #183).
    """

    @pytest.mark.timeout(5)
    def test_warns_on_backoff_limit_per_index(self, capsys):
        job = _make_indexed_job(completions=2)
        job["spec"]["backoffLimitPerIndex"] = 4
        job["spec"]["maxFailedIndexes"] = 10

        k8s_indexed_job_to_armada(job, queue="q", job_set_id="hex")

        out = capsys.readouterr().out
        assert "backoffLimitPerIndex=4" in out
        assert "maxFailedIndexes=10" in out

    @pytest.mark.timeout(5)
    def test_no_warning_when_no_retries_were_granted(self, capsys):
        """backoffLimit=0 grants no retries, so dropping it loses nothing."""
        job = _make_simple_job()
        job["spec"]["backoffLimit"] = 0

        k8s_job_to_armada(job, queue="q", job_set_id="s")

        assert "Dropping k8s Job-level retry settings" not in capsys.readouterr().out

    @pytest.mark.timeout(5)
    def test_no_warning_when_retry_fields_absent(self, capsys):
        k8s_job_to_armada(_make_simple_job(), queue="q", job_set_id="s")
        assert "Dropping k8s Job-level retry settings" not in capsys.readouterr().out

    @pytest.mark.timeout(5)
    def test_preemptible_warning_names_the_lost_unit(self, capsys):
        """
        Dropped retries plus preemptible priority is the trap from #183: a
        preempted Armada job is not rescheduled, so the whole unit is lost.
        """
        job = _make_indexed_job(completions=2)
        job["spec"]["backoffLimitPerIndex"] = 4

        k8s_indexed_job_to_armada(
            job, queue="q", job_set_id="hex", priority_class="preemptible"
        )

        out = capsys.readouterr().out
        assert "not rescheduled" in out
        assert "--armada-priority-class default" in out

    @pytest.mark.timeout(5)
    def test_no_preemptible_note_at_default_priority(self, capsys):
        job = _make_indexed_job(completions=2)
        job["spec"]["backoffLimitPerIndex"] = 4

        k8s_indexed_job_to_armada(job, queue="q", job_set_id="hex")

        out = capsys.readouterr().out
        assert "backoffLimitPerIndex=4" in out
        assert "not rescheduled" not in out


class TestSaveArmadaYaml:
    @pytest.mark.timeout(5)
    def test_saves_valid_yaml(self):
        spec = {"queue": "q", "jobSetId": "s", "jobs": [{"namespace": "n"}]}
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            save_armada_yaml(spec, f.name)
            with open(f.name) as fh:
                loaded = yaml.safe_load(fh)
            assert loaded["queue"] == "q"
            assert loaded["jobSetId"] == "s"


class TestConvertWorkflowToArmada:
    @pytest.mark.timeout(30)
    def test_converts_vector_workflow(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
            )

            armada_files = convert_workflow_to_armada(
                k8s_yaml_dir=tmpdir,
                dataset_name="test-ds",
                queue="biodiversity",
            )

            assert len(armada_files) > 0

            # Check that Armada files are valid YAML with expected structure
            for fpath in armada_files:
                with open(fpath) as f:
                    spec = yaml.safe_load(f)
                assert "queue" in spec
                assert "jobSetId" in spec
                assert "jobs" in spec
                assert len(spec["jobs"]) >= 1

    @pytest.mark.timeout(30)
    def test_hex_job_expanded_to_multiple_armada_jobs(self, monkeypatch):
        # Pin the feature count so this exercises multi-completion expansion
        # independently of the fixture's actual size and of the chunk-sizing
        # policy (issue #144): 5000 features / 1000-per-chunk target -> 5 chunks.
        import cng_datasets.k8s.workflows as wf
        monkeypatch.setattr(wf, "_count_source_features", lambda *a, **k: 5000)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
                h3_resolution=10,  # skip network geometry-type detection
            )

            convert_workflow_to_armada(
                k8s_yaml_dir=tmpdir,
                dataset_name="test-ds",
                queue="biodiversity",
            )

            hex_armada = Path(tmpdir) / "armada-test-ds-hex.yaml"
            assert hex_armada.exists()

            with open(hex_armada) as f:
                spec = yaml.safe_load(f)

            # 5000 features -> 5 completions -> 5 Armada jobs
            assert len(spec["jobs"]) == 5

            # No JOB_COMPLETION_INDEX references should remain
            full_text = str(spec)
            assert "JOB_COMPLETION_INDEX" not in full_text

    @pytest.mark.timeout(30)
    def test_single_pod_jobs_have_one_armada_job(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
            )

            convert_workflow_to_armada(
                k8s_yaml_dir=tmpdir,
                dataset_name="test-ds",
                queue="biodiversity",
            )

            for step in ["setup-bucket", "convert", "pmtiles", "repartition"]:
                fpath = Path(tmpdir) / f"armada-test-ds-{step}.yaml"
                assert fpath.exists(), f"Missing armada file for {step}"
                with open(fpath) as f:
                    spec = yaml.safe_load(f)
                assert len(spec["jobs"]) == 1, f"{step} should have exactly 1 Armada job"

    @pytest.mark.timeout(30)
    def test_priority_class_forwarded_to_every_job(self, monkeypatch):
        """
        convert_workflow_to_armada must forward priority_class, otherwise the
        class cannot be chosen from the CLI at all (issue #183, finding 3).
        """
        import cng_datasets.k8s.workflows as wf
        monkeypatch.setattr(wf, "_count_source_features", lambda *a, **k: 5000)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
                h3_resolution=10,
            )

            armada_files = convert_workflow_to_armada(
                k8s_yaml_dir=tmpdir,
                dataset_name="test-ds",
                queue="geo-workflows",
                priority_class="preemptible",
            )

            assert armada_files
            for fpath in armada_files:
                with open(fpath) as f:
                    spec = yaml.safe_load(f)
                for job in spec["jobs"]:
                    assert job["priorityClassName"] == "armada-preemptible"

    @pytest.mark.timeout(30)
    def test_default_priority_class_is_non_preemptible(self, monkeypatch):
        import cng_datasets.k8s.workflows as wf
        monkeypatch.setattr(wf, "_count_source_features", lambda *a, **k: 5000)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
                h3_resolution=10,
            )

            armada_files = convert_workflow_to_armada(
                k8s_yaml_dir=tmpdir,
                dataset_name="test-ds",
                queue="geo-workflows",
            )

            for fpath in armada_files:
                with open(fpath) as f:
                    spec = yaml.safe_load(f)
                for job in spec["jobs"]:
                    assert job["priorityClassName"] == DEFAULT_ARMADA_PRIORITY_CLASS

    @pytest.mark.timeout(30)
    def test_skips_non_job_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
            )

            armada_files = convert_workflow_to_armada(
                k8s_yaml_dir=tmpdir,
                dataset_name="test-ds",
                queue="biodiversity",
            )

            names = [Path(f).name for f in armada_files]
            assert "armada-workflow.yaml" not in names
            assert "armada-configmap.yaml" not in names
            assert "armada-workflow-rbac.yaml" not in names


class TestWorkflowBackendFlag:
    @pytest.mark.timeout(30)
    def test_backend_k8s_produces_standard_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
                backend="k8s",
            )
            assert (Path(tmpdir) / "workflow.yaml").exists()
            assert (Path(tmpdir) / "configmap.yaml").exists()
            # No armada files
            assert not list(Path(tmpdir).glob("armada-*.yaml"))

    @pytest.mark.timeout(30)
    def test_backend_armada_produces_armada_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
                backend="armada",
            )
            armada_files = list(Path(tmpdir).glob("armada-*.yaml"))
            assert len(armada_files) > 0

    @pytest.mark.timeout(5)
    def test_raster_armada_priority_class_flag(self):
        """--armada-priority-class reaches the generated raster Armada YAML."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-raster",
                source_urls="https://example.com/tile.tif",
                bucket="test-bucket",
                output_dir=tmpdir,
                backend="armada",
                armada_priority_class="high",
            )

            hex_file = Path(tmpdir) / "armada-test-raster-hex.yaml"
            with open(hex_file) as f:
                spec = yaml.safe_load(f)
            assert spec["jobs"]
            for job in spec["jobs"]:
                assert job["priorityClassName"] == "armada-high-priority"

    @pytest.mark.timeout(5)
    def test_raster_backend_armada(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-raster",
                source_urls="https://example.com/tile.tif",
                bucket="test-bucket",
                output_dir=tmpdir,
                backend="armada",
            )
            armada_files = list(Path(tmpdir).glob("armada-*.yaml"))
            assert len(armada_files) > 0

            # Hex job should have 122 Armada jobs (one per h0)
            hex_file = Path(tmpdir) / "armada-test-raster-hex.yaml"
            assert hex_file.exists()
            with open(hex_file) as f:
                spec = yaml.safe_load(f)
            assert len(spec["jobs"]) == 122


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
