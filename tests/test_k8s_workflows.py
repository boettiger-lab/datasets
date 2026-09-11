"""
Unit tests for K8s workflow generation.
"""

import tempfile
import yaml
from pathlib import Path
import pytest

from cng_datasets.k8s import generate_dataset_workflow, K8sJobManager, load_profile, cluster_config_from_args, ClusterConfig
from cng_datasets.k8s.workflows import generate_raster_workflow


class TestK8sJobManager:
    """Test Kubernetes job manager functionality."""
    
    @pytest.mark.timeout(5)
    def test_basic_job_generation(self):
        """Test generating a basic Kubernetes job."""
        manager = K8sJobManager(namespace="test-ns")
        
        job_spec = manager.generate_job_yaml(
            job_name="test-job",
            command=["python", "-c"],
            args=["print('hello')"],
            cpu="1",
            memory="2Gi"
        )
        
        assert job_spec["metadata"]["name"] == "test-job"
        assert job_spec["metadata"]["namespace"] == "test-ns"
        assert job_spec["spec"]["template"]["spec"]["containers"][0]["command"] == ["python", "-c"]
        assert job_spec["spec"]["template"]["spec"]["containers"][0]["args"] == ["print('hello')"]
        
    @pytest.mark.timeout(5)
    def test_chunked_job_generation(self):
        """Test generating an indexed/chunked job."""
        manager = K8sJobManager(namespace="test-ns")
        
        job_spec = manager.generate_chunked_job(
            job_name="chunked-job",
            script_path="/app/process.py",
            num_chunks=10,
            base_args=["--input", "data.parquet"],
            cpu="2",
            memory="4Gi",
            parallelism=5
        )
        
        assert job_spec["metadata"]["name"] == "chunked-job"
        assert job_spec["spec"]["completions"] == 10
        assert job_spec["spec"]["parallelism"] == 5
        assert job_spec["spec"]["completionMode"] == "Indexed"
        
    @pytest.mark.timeout(5)
    def test_job_yaml_save(self):
        """Test saving job spec to YAML file."""
        manager = K8sJobManager()
        
        job_spec = manager.generate_job_yaml(
            job_name="save-test",
            command=["echo", "test"]
        )
        
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test-job.yaml"
            manager.save_job_yaml(job_spec, str(output_path))
            
            assert output_path.exists()
            
            # Verify YAML is valid
            with open(output_path) as f:
                loaded = yaml.safe_load(f)
                assert loaded["metadata"]["name"] == "save-test"


class TestWorkflowGeneration:
    """Test complete workflow generation."""
    
    @pytest.mark.timeout(5)
    def test_generate_complete_workflow(self):
        """Test generating all workflow files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-dataset",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
                namespace="test-ns"
            )
            
            output_path = Path(tmpdir)
            
            # Check all expected files exist
            expected_files = [
                "test-dataset-convert.yaml",
                "test-dataset-pmtiles.yaml",
                "test-dataset-hex.yaml",
                "test-dataset-repartition.yaml",
                "workflow-rbac.yaml",
                "workflow.yaml"
            ]
            
            for filename in expected_files:
                filepath = output_path / filename
                assert filepath.exists(), f"Missing file: {filename}"
                
                # Verify YAML is valid
                with open(filepath) as f:
                    loaded = yaml.safe_load_all(f)
                    specs = list(loaded)
                    assert len(specs) > 0, f"Empty YAML file: {filename}"
    
    @pytest.mark.timeout(5)
    def test_convert_job_content(self):
        """Test convert job has correct content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir
            )
            
            convert_file = Path(tmpdir) / "test-ds-convert.yaml"
            with open(convert_file) as f:
                job = yaml.safe_load(f)
                
            assert job["metadata"]["name"] == "test-ds-convert"
            
            # Check that the command uses bash
            command = job["spec"]["template"]["spec"]["containers"][0]["command"]
            assert "bash" in command or "sh" in command
            
            # Check that S3 bucket is referenced in args or command
            container_spec = job["spec"]["template"]["spec"]["containers"][0]
            all_text = str(container_spec)
            assert "test-bucket" in all_text
    
    @pytest.mark.timeout(5)
    def test_convert_command_quotes_source_url(self):
        """Source URLs with '&' query strings must be quoted so bash -c does
        not split the command on job-control operators. Regression for #147."""
        from cng_datasets.k8s.workflows import _generate_convert_job

        url = "https://example.com/data?format=shp&a=1&b=2"
        with tempfile.TemporaryDirectory() as tmpdir:
            manager = K8sJobManager(namespace="biodiversity")
            _generate_convert_job(
                manager, "mre-test", url, "public-test",
                Path(tmpdir), "https://github.com/x/y",
            )
            job = yaml.safe_load((Path(tmpdir) / "mre-test-convert.yaml").read_text())

        convert_cmd = job["spec"]["template"]["spec"]["containers"][0]["command"][-1]
        # The URL is passed as a single quoted argument (shlex.quote uses '...').
        assert f"'{url}'" in convert_cmd
        # And never appears bare, where bash would background on the first '&'.
        assert f"\n  {url}" not in convert_cmd

    @pytest.mark.timeout(5)
    def test_hex_job_chunked(self):
        """Test hex job uses automatic chunking (defaults when bucket doesn't exist)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir
            )
            
            hex_file = Path(tmpdir) / "test-ds-hex.yaml"
            with open(hex_file) as f:
                job = yaml.safe_load(f)
                
            assert job["metadata"]["name"] == "test-ds-hex"
            # 5-feature fixture -> chunk_size floored at 1000 -> 1 chunk (#144),
            # not 5 tiny one-feature pods.
            assert job["spec"]["completions"] == 1
            assert job["spec"]["parallelism"] == 1
            assert job["spec"]["completionMode"] == "Indexed"
    
    @pytest.mark.timeout(30)
    def test_hex_job_resolution_by_area(self):
        """Issue #98: --resolution-by-area is emitted into the hex job command in
        place of --resolution, and geometry-type auto-detection is skipped."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="byarea-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
                resolution_by_area="12:8,600:6,5",
                parent_resolutions=[7, 6, 5, 4, 0],
            )
            hex_file = Path(tmpdir) / "byarea-ds-hex.yaml"
            with open(hex_file) as f:
                job = yaml.safe_load(f)
            command_str = str(job["spec"]["template"]["spec"]["containers"][0]["command"])
            assert '--resolution-by-area "12:8,600:6,5"' in command_str
            assert "--resolution " not in command_str
            assert "--parent-resolutions 7,6,5,4,0" in command_str

    def test_resolution_by_area_rejects_bad_spec(self):
        """A malformed spec fails fast during workflow generation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError):
                generate_dataset_workflow(
                    dataset_name="bad-ds",
                    source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                    bucket="test-bucket",
                    output_dir=tmpdir,
                    resolution_by_area="12:8,600:6",  # no catch-all
                )

    @pytest.mark.timeout(30)
    @pytest.mark.integration
    def test_hex_job_real_bucket(self):
        """Test hex job with real public bucket calculates proper chunking."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="mappinginequality",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="public-mappinginequality",
                output_dir=tmpdir
            )
            
            hex_file = Path(tmpdir) / "mappinginequality-hex.yaml"
            with open(hex_file) as f:
                job = yaml.safe_load(f)
                
            assert job["metadata"]["name"] == "mappinginequality-hex"
            # 5-feature fixture -> chunk_size floored at 1000 -> 1 chunk (#144).
            assert job["spec"]["completions"] == 1
            assert job["spec"]["parallelism"] == 1
            assert job["spec"]["completionMode"] == "Indexed"

            # Check chunk-size is set correctly
            command = job["spec"]["template"]["spec"]["containers"][0]["command"]
            command_str = str(command)
            assert "--chunk-size 1000" in command_str

    @pytest.mark.timeout(5)
    def test_pmtiles_job_memory(self):
        """Test that pmtiles job uses specified memory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
                hex_memory="16Gi"
            )
            
            pmtiles_file = Path(tmpdir) / "test-ds-pmtiles.yaml"
            with open(pmtiles_file) as f:
                job = yaml.safe_load(f)
                
            resources = job["spec"]["template"]["spec"]["containers"][0]["resources"]
            assert resources["requests"]["memory"] == "16Gi"
            assert resources["limits"]["memory"] == "16Gi"
    
    @pytest.mark.timeout(5)
    def test_pmtiles_job_wrapdateline(self):
        """Test that pmtiles job includes -wrapdateline flag to handle antimeridian-crossing geometries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
            )

            pmtiles_file = Path(tmpdir) / "test-ds-pmtiles.yaml"
            with open(pmtiles_file) as f:
                job = yaml.safe_load(f)

            command = job["spec"]["template"]["spec"]["containers"][0]["command"]
            command_str = str(command)
            assert "-wrapdateline" in command_str
            assert "-datelineoffset 15" in command_str

    def test_pmtiles_job_exports_tippecanoe_max_threads(self):
        """TIPPECANOE_MAX_THREADS must be exported so the child tippecanoe sees it (#77)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
            )

            pmtiles_file = Path(tmpdir) / "test-ds-pmtiles.yaml"
            with open(pmtiles_file) as f:
                job = yaml.safe_load(f)

            command = job["spec"]["template"]["spec"]["containers"][0]["command"][2]
            # A plain (un-exported) assignment is invisible to the child process.
            assert "export TIPPECANOE_MAX_THREADS=" in command

    def test_pmtiles_job_raises_open_file_limit(self):
        """Soft fd limit must be raised before tippecanoe on high-core nodes (#154)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
            )

            pmtiles_file = Path(tmpdir) / "test-ds-pmtiles.yaml"
            with open(pmtiles_file) as f:
                job = yaml.safe_load(f)

            command = job["spec"]["template"]["spec"]["containers"][0]["command"][2]
            # ulimit must precede tippecanoe, and tolerate an unraisable limit.
            assert "ulimit -n" in command
            assert command.index("ulimit -n") < command.index("tippecanoe -o")

    @pytest.mark.timeout(5)
    def test_pmtiles_max_zoom_derived_from_h3_resolution(self):
        """PMTiles max zoom defaults to h3_resolution+3; --extend-zooms removed (#133)."""
        from cng_datasets.k8s.workflows import _pmtiles_max_zoom
        assert _pmtiles_max_zoom(10, None) == 13
        assert _pmtiles_max_zoom(9, None) == 12
        assert _pmtiles_max_zoom(8, None) == 11
        assert _pmtiles_max_zoom(None, None) == 13   # fallback default
        assert _pmtiles_max_zoom(10, 16) == 16       # explicit override wins

    @pytest.mark.timeout(5)
    def test_pmtiles_job_no_extend_zooms(self):
        """tippecanoe command must not contain --extend-zooms-if-still-dropping (#133)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
                h3_resolution=10,
            )
            pmtiles_file = Path(tmpdir) / "test-ds-pmtiles.yaml"
            with open(pmtiles_file) as f:
                job = yaml.safe_load(f)
            command = job["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert "--extend-zooms-if-still-dropping" not in command
            assert "-z 13" in command
            assert "--coalesce-densest-as-needed" in command

    @pytest.mark.timeout(5)
    def test_pmtiles_max_zoom_override(self):
        """pmtiles_max_zoom kwarg overrides the H3-derived default (#133)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
                h3_resolution=10,
                pmtiles_max_zoom=16,
            )
            pmtiles_file = Path(tmpdir) / "test-ds-pmtiles.yaml"
            with open(pmtiles_file) as f:
                job = yaml.safe_load(f)
            command = job["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert "-z 16" in command

    @pytest.mark.timeout(5)
    def test_workflow_rbac(self):
        """Test RBAC configuration is generated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
                namespace="custom-ns"
            )
            
            rbac_file = Path(tmpdir) / "workflow-rbac.yaml"
            with open(rbac_file) as f:
                docs = list(yaml.safe_load_all(f))
                
            # Should have ServiceAccount, Role, and RoleBinding
            assert len(docs) == 3
            
            kinds = [doc["kind"] for doc in docs]
            assert "ServiceAccount" in kinds
            assert "Role" in kinds
            assert "RoleBinding" in kinds
            
            # Check namespace
            for doc in docs:
                assert doc["metadata"]["namespace"] == "custom-ns"

    @pytest.mark.timeout(5)
    def test_repartition_job_ephemeral_storage(self):
        """Repartition job must request ephemeral-storage to avoid eviction on large datasets."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
            )
            job_file = Path(tmpdir) / "test-ds-repartition.yaml"
            with open(job_file) as f:
                job = yaml.safe_load(f)
            resources = job["spec"]["template"]["spec"]["containers"][0]["resources"]
            assert resources["requests"]["ephemeral-storage"] == "50Gi"
            assert resources["limits"]["ephemeral-storage"] == "50Gi"

    @pytest.mark.timeout(5)
    def test_repartition_job_custom_ephemeral_storage(self):
        """--repartition-storage should override the default ephemeral-storage value."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
                repartition_storage="400Gi",
            )
            job_file = Path(tmpdir) / "test-ds-repartition.yaml"
            with open(job_file) as f:
                job = yaml.safe_load(f)
            resources = job["spec"]["template"]["spec"]["containers"][0]["resources"]
            assert resources["requests"]["ephemeral-storage"] == "400Gi"
            assert resources["limits"]["ephemeral-storage"] == "400Gi"

    @pytest.mark.timeout(5)
    def test_repartition_job_memory(self):
        """repartition_memory controls pod memory and DuckDB memory_limit (85%)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
                repartition_memory="64Gi",
            )
            job_file = Path(tmpdir) / "test-ds-repartition.yaml"
            with open(job_file) as f:
                job = yaml.safe_load(f)
            resources = job["spec"]["template"]["spec"]["containers"][0]["resources"]
            assert resources["requests"]["memory"] == "64Gi"
            assert resources["limits"]["memory"] == "64Gi"
            # DuckDB limit should be 85% = 54GiB
            command = job["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert "--memory-limit 54GiB" in command


class TestCalculateChunking:
    """Issue #144: right-size hex `completions` to the chunks that hold data
    instead of shredding small datasets into ~max_completions tiny pods."""

    def test_small_dataset_uses_few_chunks(self):
        """5,697 features -> 6 chunks of 1000, not ~200 tiny pods (the #144 repro)."""
        from cng_datasets.k8s.workflows import _calculate_chunking
        chunk_size, completions, parallelism = _calculate_chunking(5697, max_completions=200)
        assert chunk_size == 1000
        assert completions == 6
        assert parallelism == 6

    def test_tiny_dataset_is_single_chunk(self):
        """A handful of features -> one pod, not one pod per feature."""
        from cng_datasets.k8s.workflows import _calculate_chunking
        assert _calculate_chunking(5, max_completions=200) == (1000, 1, 1)

    def test_large_dataset_caps_at_max_completions(self):
        """Beyond max_completions*target, chunk_size grows to cap completions."""
        from cng_datasets.k8s.workflows import _calculate_chunking
        # 711,583 features: target-1000 would need 712 > 200 chunks, so chunk_size
        # grows to ceil(711583/200)=3558, completions caps at 200.
        chunk_size, completions, parallelism = _calculate_chunking(711583, max_completions=200)
        assert chunk_size == 3558
        assert completions == 200
        assert parallelism == 50
        # Crucially, completions*chunk_size still covers every feature (no #170 truncation).
        assert completions * chunk_size >= 711583

    def test_boundary_at_target_times_max_completions(self):
        """Exactly max_completions*target features stays at the target chunk size."""
        from cng_datasets.k8s.workflows import _calculate_chunking
        assert _calculate_chunking(200000, max_completions=200) == (1000, 200, 50)
        # One more feature tips chunk_size up so completions stays <= max.
        chunk_size, completions, _ = _calculate_chunking(200001, max_completions=200)
        assert completions <= 200
        assert chunk_size * completions >= 200001

    def test_covers_all_features_across_scales(self):
        """completions*chunk_size must always cover the dataset (no silent drop)."""
        from cng_datasets.k8s.workflows import _calculate_chunking
        for n in (1, 5, 999, 1000, 1001, 5697, 199999, 200000, 200001, 1_000_000):
            chunk_size, completions, parallelism = _calculate_chunking(n, max_completions=200)
            assert completions >= 1
            assert completions <= 200
            assert chunk_size * completions >= n, f"n={n} not fully covered"


class TestSimplifyToleranceWiring:
    """Issue #132: --simplify-tolerance reaches the generated convert job command."""

    def test_convert_job_includes_simplify_flag(self, monkeypatch):
        import cng_datasets.k8s.workflows as wf
        monkeypatch.setattr(wf, "_count_source_features", lambda *a, **k: 5000)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="simp-ds",
                source_url="https://example.com/big.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
                h3_resolution=10,           # skip network geometry detection
                simplify_tolerance=0.0001,
            )
            convert_yaml = yaml.safe_load(open(Path(tmpdir) / "simp-ds-convert.yaml"))
            cmd = str(convert_yaml["spec"]["template"]["spec"]["containers"][0]["command"])
            assert "--simplify-tolerance 0.0001" in cmd

    def test_convert_job_omits_flag_by_default(self, monkeypatch):
        import cng_datasets.k8s.workflows as wf
        monkeypatch.setattr(wf, "_count_source_features", lambda *a, **k: 5000)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="nosimp-ds",
                source_url="https://example.com/big.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
                h3_resolution=10,
            )
            convert_yaml = yaml.safe_load(open(Path(tmpdir) / "nosimp-ds-convert.yaml"))
            cmd = str(convert_yaml["spec"]["template"]["spec"]["containers"][0]["command"])
            assert "--simplify-tolerance" not in cmd


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    @pytest.mark.timeout(5)
    def test_invalid_dataset_name(self):
        """Test with dataset name containing special characters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Should handle this gracefully
            generate_dataset_workflow(
                dataset_name="my-dataset-2024",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir
            )
            
            # Check files were created
            assert (Path(tmpdir) / "my-dataset-2024-convert.yaml").exists()
    
    @pytest.mark.timeout(5)
    def test_output_dir_created(self):
        """Test that output directory is created if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "subdir" / "workflow"
            
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg",
                bucket="test-bucket",
                output_dir=str(output_dir)
            )
            
            assert output_dir.exists()
            assert (output_dir / "test-ds-convert.yaml").exists()

    @pytest.mark.timeout(10)
    def test_multi_source_workflow(self, mocker):
        """Test generating workflow with multiple source URLs."""
        # Mock feature counting to avoid trying to access non-existent URLs
        mocker.patch('cng_datasets.k8s.workflows._count_source_features', return_value=10000)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Test with list of URLs
            generate_dataset_workflow(
                dataset_name="multi-test",
                source_urls=[
                    "https://example.com/region1.shp",
                    "https://example.com/region2.shp",
                    "https://example.com/region3.shp"
                ],
                bucket="test-bucket",
                output_dir=tmpdir,
                namespace="test-ns"
            )
            
            output_path = Path(tmpdir)
            convert_file = output_path / "multi-test-convert.yaml"
            
            assert convert_file.exists(), "Convert job file should exist"
            
            # Verify YAML is valid
            with open(convert_file) as f:
                job = yaml.safe_load(f)
            
            # Check that the command contains all three source URLs
            container_spec = job["spec"]["template"]["spec"]["containers"][0]
            command_str = str(container_spec)
            
            assert "region1.shp" in command_str
            assert "region2.shp" in command_str
            assert "region3.shp" in command_str
            
    @pytest.mark.timeout(10)
    def test_single_source_as_string(self, mocker):
        """Test that single source URL as string still works (backwards compatibility)."""
        # Mock feature counting to avoid trying to access non-existent URLs
        mocker.patch('cng_datasets.k8s.workflows._count_source_features', return_value=5000)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Test with single string URL (not a list)
            generate_dataset_workflow(
                dataset_name="single-test",
                source_urls="https://example.com/single.shp",
                bucket="test-bucket",
                output_dir=tmpdir,
                namespace="test-ns"
            )
            
            output_path = Path(tmpdir)
            convert_file = output_path / "single-test-convert.yaml"
            
            assert convert_file.exists(), "Convert job file should exist"
            
            # Verify YAML is valid
            with open(convert_file) as f:
                job = yaml.safe_load(f)
            
            # Check that the command contains the source URL
            container_spec = job["spec"]["template"]["spec"]["containers"][0]
            command_str = str(container_spec)
            
            assert "single.shp" in command_str

    @pytest.mark.timeout(10)
    def test_feature_count_fallback_uses_conservative_chunk_size(self, mocker):
        """When feature counting fails, chunk_size should be large enough to cover large datasets."""
        mocker.patch(
            'cng_datasets.k8s.workflows._count_source_features',
            side_effect=Exception("ogrinfo timed out after 30 seconds")
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="fallback-test",
                source_urls="https://example.com/large.gdb",
                bucket="test-bucket",
                output_dir=tmpdir,
                max_completions=200,
            )

            hex_file = Path(tmpdir) / "fallback-test-hex.yaml"
            with open(hex_file) as f:
                job = yaml.safe_load(f)

            command_str = str(job["spec"]["template"]["spec"]["containers"][0]["command"])
            # With max_completions=200, fallback total_rows=200*1000=200000,
            # so chunk_size=ceil(200000/200)=1000 — not the old silently-small 50.
            assert "--chunk-size 50" not in command_str
            assert "--chunk-size 1000" in command_str


class TestRasterWorkflowGeneration:
    """Tests for generate_raster_workflow(), especially the multi-tile mosaic path."""

    SOURCE_URL = "https://example.com/tile.tif"
    TILE_URLS = [
        "https://example.com/zone12-tile1.tif",
        "https://example.com/zone12-tile2.tif",
        "https://example.com/zone13-tile1.tif",
    ]

    def _load_yaml(self, path):
        with open(path) as f:
            return yaml.safe_load(f)

    @pytest.mark.timeout(5)
    def test_single_url_no_preprocess_job(self):
        """Single source URL → no preprocess-cog.yaml should be generated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-raster",
                source_urls=self.SOURCE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
            )
            assert not (Path(tmpdir) / "test-raster-preprocess-cog.yaml").exists()
            assert (Path(tmpdir) / "test-raster-hex.yaml").exists()

    @pytest.mark.timeout(5)
    def test_multi_url_generates_preprocess_job(self):
        """Multiple source URLs → preprocess-cog.yaml should be generated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-mosaic",
                source_urls=self.TILE_URLS,
                bucket="test-bucket",
                output_dir=tmpdir,
            )
            assert (Path(tmpdir) / "test-mosaic-preprocess-cog.yaml").exists()
            assert (Path(tmpdir) / "test-mosaic-hex.yaml").exists()

    @pytest.mark.timeout(5)
    def test_target_extent_triggers_preprocess(self):
        """Single URL + target_extent → preprocess-cog.yaml should be generated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-clip",
                source_urls=self.SOURCE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
                target_extent=(-111.1, 40.9, -104.0, 45.1),
            )
            assert (Path(tmpdir) / "test-clip-preprocess-cog.yaml").exists()

    @pytest.mark.timeout(5)
    def test_band_triggers_preprocess(self):
        """Single URL + band → preprocess-cog.yaml should be generated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-band",
                source_urls=self.SOURCE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
                band=4,
            )
            assert (Path(tmpdir) / "test-band-preprocess-cog.yaml").exists()

    @pytest.mark.timeout(5)
    def test_preprocess_job_all_input_urls_present(self):
        """Preprocess job command must include all source tile URLs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-mosaic",
                source_urls=self.TILE_URLS,
                bucket="test-bucket",
                output_dir=tmpdir,
            )
            job = self._load_yaml(Path(tmpdir) / "test-mosaic-preprocess-cog.yaml")
            command_str = str(job["spec"]["template"]["spec"]["containers"][0]["command"])
            for url in self.TILE_URLS:
                assert url in command_str, f"Expected tile URL {url!r} in preprocess command"

    @pytest.mark.timeout(5)
    def test_preprocess_job_output_cog_url(self):
        """Preprocess job command must reference the correct S3 output COG URL."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-mosaic",
                source_urls=self.TILE_URLS,
                bucket="my-bucket",
                output_dir=tmpdir,
            )
            job = self._load_yaml(Path(tmpdir) / "test-mosaic-preprocess-cog.yaml")
            command_str = str(job["spec"]["template"]["spec"]["containers"][0]["command"])
            assert "s3://my-bucket/test-mosaic-cog.tif" in command_str

    @pytest.mark.timeout(5)
    def test_preprocess_job_custom_cog_name(self):
        """output_cog_name overrides the default COG filename."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-mosaic",
                source_urls=self.TILE_URLS,
                bucket="my-bucket",
                output_dir=tmpdir,
                output_cog_name="custom-name.tif",
            )
            job = self._load_yaml(Path(tmpdir) / "test-mosaic-preprocess-cog.yaml")
            command_str = str(job["spec"]["template"]["spec"]["containers"][0]["command"])
            assert "s3://my-bucket/custom-name.tif" in command_str

    @pytest.mark.timeout(5)
    def test_preprocess_job_resources(self):
        """Preprocess job must request sufficient CPU/memory/storage for mosaicking."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-mosaic",
                source_urls=self.TILE_URLS,
                bucket="test-bucket",
                output_dir=tmpdir,
            )
            job = self._load_yaml(Path(tmpdir) / "test-mosaic-preprocess-cog.yaml")
            resources = job["spec"]["template"]["spec"]["containers"][0]["resources"]
            assert resources["requests"]["cpu"] == "8"
            assert resources["requests"]["memory"] == "32Gi"
            assert resources["requests"]["ephemeral-storage"] == "50Gi"

    @pytest.mark.timeout(5)
    def test_preprocess_job_target_extent_flag(self):
        """target_extent should appear in preprocess job command as --target-extent."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-clip",
                source_urls=self.SOURCE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
                target_extent=(-111.1, 40.9, -104.0, 45.1),
            )
            job = self._load_yaml(Path(tmpdir) / "test-clip-preprocess-cog.yaml")
            command_str = str(job["spec"]["template"]["spec"]["containers"][0]["command"])
            assert "--target-extent" in command_str
            assert "-111.1" in command_str
            assert "45.1" in command_str

    @pytest.mark.timeout(5)
    def test_preprocess_job_band_flag(self):
        """band should appear in preprocess job command as --band."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-band",
                source_urls=self.SOURCE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
                band=4,
            )
            job = self._load_yaml(Path(tmpdir) / "test-band-preprocess-cog.yaml")
            command_str = str(job["spec"]["template"]["spec"]["containers"][0]["command"])
            assert "--band 4" in command_str

    @pytest.mark.timeout(5)
    def test_preprocess_job_multi_value_nodata_and_hex_resampling(self):
        """Categorical multi-fill nodata + reducer flow into the COG step (issue #108)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-mosaic",
                source_urls=self.TILE_URLS,
                bucket="test-bucket",
                output_dir=tmpdir,
                nodata_value="-9999,-1111,32767",
                hex_resampling="mode",
            )
            job = self._load_yaml(Path(tmpdir) / "test-mosaic-preprocess-cog.yaml")
            cmd = job["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert '--nodata "-9999,-1111,32767"' in cmd
            assert "--hex-resampling mode" in cmd
            # Regression guard: --resampling must keep its line continuation so
            # the appended flags stay part of the same shell command.
            assert "--resampling bilinear \\\n" in cmd

    @pytest.mark.timeout(5)
    def test_preprocess_job_single_value_nodata_normalized(self):
        """A single float nodata renders as a clean integer string, no trailing .0."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-mosaic",
                source_urls=self.TILE_URLS,
                bucket="test-bucket",
                output_dir=tmpdir,
                nodata_value=255.0,
            )
            job = self._load_yaml(Path(tmpdir) / "test-mosaic-preprocess-cog.yaml")
            cmd = job["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert '--nodata "255"' in cmd

    @pytest.mark.timeout(5)
    def test_hex_job_emits_multi_value_nodata(self):
        """The hex job excludes every fill code (issue #108)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-raster",
                source_urls=self.SOURCE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
                nodata_value="-9999,-1111,32767",
            )
            hex_job = self._load_yaml(Path(tmpdir) / "test-raster-hex.yaml")
            cmd = hex_job["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert '--nodata "-9999,-1111,32767"' in cmd

    @pytest.mark.timeout(5)
    def test_hex_job_gets_primary_nodata_when_preprocess(self):
        """When a COG preprocess collapses fills, the hex job needs only the primary value."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-mosaic",
                source_urls=self.TILE_URLS,  # multi-tile → preprocess-cog runs
                bucket="test-bucket",
                output_dir=tmpdir,
                nodata_value="-9999,-1111,32767",
            )
            # Preprocess collapses all fills → COG carries one nodata.
            pre = self._load_yaml(Path(tmpdir) / "test-mosaic-preprocess-cog.yaml")
            pre_cmd = pre["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert '--nodata "-9999,-1111,32767"' in pre_cmd
            # Hex only excludes the primary; it must not re-remap the full list.
            hex_job = self._load_yaml(Path(tmpdir) / "test-mosaic-hex.yaml")
            hex_cmd = hex_job["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert '--nodata "-9999"' in hex_cmd
            assert "-1111" not in hex_cmd

    @pytest.mark.timeout(5)
    def test_hex_job_reads_from_cog_when_preprocess(self):
        """When preprocess is needed, hex job should read from the intermediate COG, not source URLs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-mosaic",
                source_urls=self.TILE_URLS,
                bucket="my-bucket",
                output_dir=tmpdir,
            )
            hex_job = self._load_yaml(Path(tmpdir) / "test-mosaic-hex.yaml")
            command_str = str(hex_job["spec"]["template"]["spec"]["containers"][0]["command"])
            # Hex job should reference the intermediate COG, not individual tile URLs
            assert "s3://my-bucket/test-mosaic-cog.tif" in command_str
            for url in self.TILE_URLS:
                assert url not in command_str, f"Hex job should not reference source tile {url!r}"

    @pytest.mark.timeout(5)
    def test_hex_job_reads_from_source_when_no_preprocess(self):
        """When no preprocess needed, hex job should reference the source URL directly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-raster",
                source_urls=self.SOURCE_URL,
                bucket="my-bucket",
                output_dir=tmpdir,
            )
            hex_job = self._load_yaml(Path(tmpdir) / "test-raster-hex.yaml")
            command_str = str(hex_job["spec"]["template"]["spec"]["containers"][0]["command"])
            assert self.SOURCE_URL in command_str

    @pytest.mark.timeout(5)
    def test_hex_resampling_default_in_hex_command(self):
        """Hex job command should include --hex-resampling with the default 'mean'."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-raster",
                source_urls=self.SOURCE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
            )
            hex_job = self._load_yaml(Path(tmpdir) / "test-raster-hex.yaml")
            command_str = str(hex_job["spec"]["template"]["spec"]["containers"][0]["command"])
            assert "--hex-resampling mean" in command_str

    @pytest.mark.timeout(5)
    def test_hex_resampling_mode_propagates_to_hex_command(self):
        """hex_resampling='mode' should appear in the generated hex job command (issue #80)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-categorical",
                source_urls=self.SOURCE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
                hex_resampling="mode",
            )
            hex_job = self._load_yaml(Path(tmpdir) / "test-categorical-hex.yaml")
            command_str = str(hex_job["spec"]["template"]["spec"]["containers"][0]["command"])
            assert "--hex-resampling mode" in command_str

    @pytest.mark.timeout(5)
    def test_raster_workflow_backwards_compat_string(self):
        """source_urls accepts a plain string for backwards compatibility."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="compat-test",
                source_urls="https://example.com/data.tif",
                bucket="test-bucket",
                output_dir=tmpdir,
            )
            assert (Path(tmpdir) / "compat-test-hex.yaml").exists()
            assert not (Path(tmpdir) / "compat-test-preprocess-cog.yaml").exists()

    @pytest.mark.timeout(5)
    def test_non_cog_single_url_triggers_preprocess(self):
        """Single non-COG source URL should auto-trigger preprocess-cog step."""
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("cng_datasets.raster.cog.is_cog", return_value=False):
                generate_raster_workflow(
                    dataset_name="test-noncog",
                    source_urls=self.SOURCE_URL,
                    bucket="test-bucket",
                    output_dir=tmpdir,
                )
            assert (Path(tmpdir) / "test-noncog-preprocess-cog.yaml").exists()
            assert (Path(tmpdir) / "test-noncog-hex.yaml").exists()
            # Hex job should read from the intermediate COG, not the raw source
            job = self._load_yaml(Path(tmpdir) / "test-noncog-hex.yaml")
            command_str = str(job["spec"]["template"]["spec"]["containers"][0]["command"])
            assert "s3://test-bucket/test-noncog-cog.tif" in command_str
            assert self.SOURCE_URL not in command_str

    @pytest.mark.timeout(5)
    def test_configmap_includes_preprocess_step(self):
        """ConfigMap workflow script should reference the preprocess-cog job when needed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-mosaic",
                source_urls=self.TILE_URLS,
                bucket="test-bucket",
                output_dir=tmpdir,
            )
            configmap = self._load_yaml(Path(tmpdir) / "configmap.yaml")
            script = str(configmap)
            assert "preprocess-cog" in script

    @pytest.mark.timeout(5)
    def test_configmap_excludes_preprocess_step_for_single_url(self):
        """ConfigMap workflow script should NOT reference preprocess-cog for single-URL case."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-raster",
                source_urls=self.SOURCE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
            )
            configmap = self._load_yaml(Path(tmpdir) / "configmap.yaml")
            script = str(configmap)
            assert "preprocess-cog" not in script

    @pytest.mark.timeout(5)
    def test_preprocess_job_no_gpu_affinity(self):
        """Preprocess job must avoid GPU nodes (CPU-only GDAL workload)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-mosaic",
                source_urls=self.TILE_URLS,
                bucket="test-bucket",
                output_dir=tmpdir,
            )
            job = self._load_yaml(Path(tmpdir) / "test-mosaic-preprocess-cog.yaml")
            affinity = job["spec"]["template"]["spec"]["affinity"]
            selector_terms = (
                affinity["nodeAffinity"]
                ["requiredDuringSchedulingIgnoredDuringExecution"]
                ["nodeSelectorTerms"]
            )
            exprs = [e for term in selector_terms for e in term.get("matchExpressions", [])]
            gpu_expr = next(
                (e for e in exprs if "pci-10de" in e.get("key", "")), None
            )
            assert gpu_expr is not None, "Should have GPU node avoidance affinity"
            assert gpu_expr["operator"] == "NotIn"


FIXTURE_URL = "https://s3-west.nrp-nautilus.io/public-test/fixtures/test-fixture.gpkg"


class TestClusterConfig:
    """Test cluster configuration customization."""

    def _all_env_vars(self, job):
        return job["spec"]["template"]["spec"]["containers"][0]["env"]

    def _env_value(self, env_list, name):
        for e in env_list:
            if e["name"] == name:
                return e.get("value") or e.get("valueFrom")
        return None

    @pytest.mark.timeout(5)
    def test_default_config_produces_nrp_values(self):
        """Default config should produce the same NRP-specific values as before."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="cfg-test",
                source_url=FIXTURE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
            )
            job = yaml.safe_load(open(Path(tmpdir) / "cfg-test-convert.yaml"))
        env = self._all_env_vars(job)
        assert self._env_value(env, "AWS_S3_ENDPOINT") == "rook-ceph-rgw-nautiluss3.rook"
        assert self._env_value(env, "AWS_PUBLIC_ENDPOINT") == "s3-west.nrp-nautilus.io"
        assert self._env_value(env, "AWS_ACCESS_KEY_ID") == {"secretKeyRef": {"name": "aws", "key": "AWS_ACCESS_KEY_ID"}}

    @pytest.mark.timeout(5)
    def test_custom_s3_endpoint(self):
        """--s3-endpoint should propagate to all job specs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="cfg-test",
                source_url=FIXTURE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
                s3_endpoint="minio.my-cluster.local",
                s3_public_endpoint="s3.my-cluster.io",
            )
            for job_file in ["cfg-test-convert.yaml", "cfg-test-hex.yaml", "cfg-test-repartition.yaml"]:
                job = yaml.safe_load(open(Path(tmpdir) / job_file))
                env = self._all_env_vars(job)
                assert self._env_value(env, "AWS_S3_ENDPOINT") == "minio.my-cluster.local", job_file
                assert self._env_value(env, "AWS_PUBLIC_ENDPOINT") == "s3.my-cluster.io", job_file

    @pytest.mark.timeout(5)
    def test_custom_s3_secret_name(self):
        """--s3-secret-name should change the secretKeyRef in all job specs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="cfg-test",
                source_url=FIXTURE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
                s3_secret_name="my-bucket-creds",
            )
            for job_file in ["cfg-test-convert.yaml", "cfg-test-hex.yaml"]:
                job = yaml.safe_load(open(Path(tmpdir) / job_file))
                env = self._all_env_vars(job)
                ref = self._env_value(env, "AWS_ACCESS_KEY_ID")
                assert ref["secretKeyRef"]["name"] == "my-bucket-creds", job_file

    @pytest.mark.timeout(5)
    def test_custom_rclone_secret_name(self):
        """--rclone-secret-name should change the volume secretName in setup-bucket and repartition."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="cfg-test",
                source_url=FIXTURE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
                rclone_secret_name="my-rclone-secret",
            )
            for job_file in ["cfg-test-setup-bucket.yaml", "cfg-test-repartition.yaml"]:
                job = yaml.safe_load(open(Path(tmpdir) / job_file))
                volumes = job["spec"]["template"]["spec"]["volumes"]
                rclone_vol = next(v for v in volumes if v["name"] == "rclone-config")
                assert rclone_vol["secret"]["secretName"] == "my-rclone-secret", job_file

    @pytest.mark.timeout(5)
    def test_custom_rclone_remote(self):
        """--rclone-remote should appear in setup-bucket command and pmtiles upload."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="cfg-test",
                source_url=FIXTURE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
                rclone_remote="my-remote",
            )
            setup_job = yaml.safe_load(open(Path(tmpdir) / "cfg-test-setup-bucket.yaml"))
            cmd = str(setup_job["spec"]["template"]["spec"]["containers"][0]["command"])
            assert "--remote my-remote" in cmd

            pmtiles_job = yaml.safe_load(open(Path(tmpdir) / "cfg-test-pmtiles.yaml"))
            cmd = str(pmtiles_job["spec"]["template"]["spec"]["containers"][0]["command"])
            assert "my-remote:" in cmd

    @pytest.mark.timeout(5)
    def test_custom_priority_class(self):
        """--priority-class should appear in all pod specs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="cfg-test",
                source_url=FIXTURE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
                priority_class="standard",
            )
            for job_file in ["cfg-test-convert.yaml", "cfg-test-hex.yaml"]:
                job = yaml.safe_load(open(Path(tmpdir) / job_file))
                pod_spec = job["spec"]["template"]["spec"]
                assert pod_spec.get("priorityClassName") == "standard", job_file

    @pytest.mark.timeout(5)
    def test_empty_priority_class_omits_field(self):
        """Empty --priority-class should omit priorityClassName from pod specs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="cfg-test",
                source_url=FIXTURE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
                priority_class="",
            )
            job = yaml.safe_load(open(Path(tmpdir) / "cfg-test-convert.yaml"))
            pod_spec = job["spec"]["template"]["spec"]
            assert "priorityClassName" not in pod_spec

    @pytest.mark.timeout(5)
    def test_node_affinity_none_omits_affinity(self):
        """--node-affinity none should omit affinity from all pod specs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="cfg-test",
                source_url=FIXTURE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
                node_affinity="none",
            )
            for job_file in ["cfg-test-convert.yaml", "cfg-test-hex.yaml", "cfg-test-repartition.yaml"]:
                job = yaml.safe_load(open(Path(tmpdir) / job_file))
                pod_spec = job["spec"]["template"]["spec"]
                assert "affinity" not in pod_spec, f"{job_file} should not have affinity"

    @pytest.mark.timeout(5)
    def test_raster_workflow_custom_config(self):
        """Raster workflow should also respect cluster config overrides."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="raster-cfg",
                source_urls="https://example.com/tile.tif",
                bucket="test-bucket",
                output_dir=tmpdir,
                s3_endpoint="minio.internal",
                s3_secret_name="minio-creds",
                node_affinity="none",
            )
            hex_job = yaml.safe_load(open(Path(tmpdir) / "raster-cfg-hex.yaml"))
            env = self._all_env_vars(hex_job)
            assert self._env_value(env, "AWS_S3_ENDPOINT") == "minio.internal"
            ref = self._env_value(env, "AWS_ACCESS_KEY_ID")
            assert ref["secretKeyRef"]["name"] == "minio-creds"
            pod_spec = hex_job["spec"]["template"]["spec"]
            assert "affinity" not in pod_spec

    @pytest.mark.timeout(5)
    def test_raster_hex_job_mounts_rclone_config(self):
        """Hex job must mount rclone-config so _localize_input can rclone-copy the COG (issue #99)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="raster-cfg",
                source_urls="https://example.com/tile.tif",
                bucket="test-bucket",
                output_dir=tmpdir,
                rclone_secret_name="my-rclone-secret",
            )
            hex_job = yaml.safe_load(open(Path(tmpdir) / "raster-cfg-hex.yaml"))
            pod_spec = hex_job["spec"]["template"]["spec"]
            rclone_vol = next(v for v in pod_spec["volumes"] if v["name"] == "rclone-config")
            assert rclone_vol["secret"]["secretName"] == "my-rclone-secret"
            mount = next(
                m for m in pod_spec["containers"][0]["volumeMounts"]
                if m["name"] == "rclone-config"
            )
            assert mount["mountPath"] == "/root/.config/rclone"


class TestProfileLoading:
    """Tests for load_profile() and cluster_config_from_args()."""

    @pytest.mark.timeout(5)
    def test_load_builtin_nrp_profile(self):
        """Built-in 'nrp' profile loads and has expected NRP values."""
        profile = load_profile("nrp")
        assert profile["s3_endpoint"] == "rook-ceph-rgw-nautiluss3.rook"
        assert profile["s3_public_endpoint"] == "s3-west.nrp-nautilus.io"
        assert profile["s3_secret_name"] == "aws"
        assert profile["rclone_remote"] == "nrp"
        # Deliberately empty, i.e. no priorityClassName, i.e. default priority
        # 0. On NRP `opportunistic` is -2000000000 and preemption exposure
        # scales with pod runtime, so it was the wrong default for the
        # multi-hour pods this profile mostly generates (issue #201).
        assert profile["priority_class"] == ""
        assert profile["node_affinity"] == "gpu-avoid"

    @pytest.mark.timeout(5)
    def test_load_profile_from_path(self):
        """load_profile() accepts a path to a YAML file."""
        profile_data = {
            "name": "test-cluster",
            "s3_endpoint": "minio.test.local",
            "s3_public_endpoint": "minio.test.io",
            "s3_secret_name": "test-creds",
            "rclone_remote": "test",
            "priority_class": "",
            "node_affinity": "none",
        }
        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
            yaml.dump(profile_data, f)
            profile_path = f.name

        profile = load_profile(profile_path)
        assert profile["s3_endpoint"] == "minio.test.local"
        assert profile["s3_secret_name"] == "test-creds"
        assert profile["node_affinity"] == "none"
        # 'name' key should be stripped (not a ClusterConfig field)
        assert "name" not in profile

    @pytest.mark.timeout(5)
    def test_load_profile_unknown_name_raises(self):
        """load_profile() raises FileNotFoundError for unknown profile names."""
        with pytest.raises(FileNotFoundError, match="not found"):
            load_profile("nonexistent-cluster")

    @pytest.mark.timeout(5)
    def test_load_profile_from_user_dir(self, tmp_path, monkeypatch):
        """load_profile() finds profiles in the user config directory."""
        import cng_datasets.k8s.workflows as wf_module
        user_profiles = tmp_path / "profiles"
        user_profiles.mkdir()
        (user_profiles / "my-cluster.yaml").write_text(
            "s3_endpoint: s3.my-org.internal\nrclone_remote: my-org\n"
        )
        monkeypatch.setattr(wf_module, "_USER_PROFILES_DIR", user_profiles)

        profile = load_profile("my-cluster")
        assert profile["s3_endpoint"] == "s3.my-org.internal"
        assert profile["rclone_remote"] == "my-org"

    @pytest.mark.timeout(5)
    def test_cluster_config_from_args_no_profile(self):
        """cluster_config_from_args with no profile uses ClusterConfig defaults."""
        cfg = cluster_config_from_args()
        assert cfg == ClusterConfig()

    @pytest.mark.timeout(5)
    def test_cluster_config_from_args_profile_only(self):
        """Profile values are applied when no CLI overrides are given."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
            yaml.dump({"s3_endpoint": "minio.example.com", "rclone_remote": "example"}, f)
            path = f.name

        cfg = cluster_config_from_args(profile=path)
        assert cfg.s3_endpoint == "minio.example.com"
        assert cfg.rclone_remote == "example"
        # Fields not in profile stay at ClusterConfig defaults
        assert cfg.s3_secret_name == "aws"

    @pytest.mark.timeout(5)
    def test_cluster_config_from_args_cli_overrides_profile(self):
        """Explicit CLI flags (non-None) override profile values."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
            yaml.dump({"s3_endpoint": "minio.example.com", "rclone_remote": "example"}, f)
            path = f.name

        cfg = cluster_config_from_args(
            profile=path,
            s3_endpoint="override.example.com",   # explicit — should win
            rclone_remote=None,                    # None — profile value wins
        )
        assert cfg.s3_endpoint == "override.example.com"
        assert cfg.rclone_remote == "example"

    @pytest.mark.timeout(5)
    def test_workflow_with_profile_flag(self):
        """generate_dataset_workflow respects the profile parameter."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
            yaml.dump({
                "s3_endpoint": "minio.profile.local",
                "s3_secret_name": "profile-creds",
                "node_affinity": "none",
            }, f)
            profile_path = f.name

        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="profile-test",
                source_url=FIXTURE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
                profile=profile_path,
            )
            job = yaml.safe_load(open(Path(tmpdir) / "profile-test-convert.yaml"))

        env = job["spec"]["template"]["spec"]["containers"][0]["env"]
        env_map = {e["name"]: e.get("value") or e.get("valueFrom") for e in env}
        assert env_map["AWS_S3_ENDPOINT"] == "minio.profile.local"
        assert env_map["AWS_ACCESS_KEY_ID"]["secretKeyRef"]["name"] == "profile-creds"
        assert "affinity" not in job["spec"]["template"]["spec"]

    @pytest.mark.timeout(5)
    def test_workflow_cli_flag_overrides_profile(self):
        """An explicit CLI flag beats a profile value for the same field."""
        with tempfile.NamedTemporaryFile(suffix=".yaml", mode="w", delete=False) as f:
            yaml.dump({"s3_endpoint": "minio.profile.local"}, f)
            profile_path = f.name

        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="override-test",
                source_url=FIXTURE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
                profile=profile_path,
                s3_endpoint="explicit.override.io",   # should win over profile
            )
            job = yaml.safe_load(open(Path(tmpdir) / "override-test-convert.yaml"))

        env = job["spec"]["template"]["spec"]["containers"][0]["env"]
        env_map = {e["name"]: e.get("value") for e in env if "value" in e}
        assert env_map["AWS_S3_ENDPOINT"] == "explicit.override.io"

    @pytest.mark.timeout(5)
    def test_builtin_nrp_profile_matches_defaults(self):
        """Loading the 'nrp' profile produces the same config as bare ClusterConfig()."""
        cfg_default = ClusterConfig()
        cfg_nrp = cluster_config_from_args(profile="nrp")
        assert cfg_nrp == cfg_default


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


class TestTrimStringsWiring:
    """Issue #180: --trim-strings reaches the generated convert job command."""

    def test_convert_job_includes_trim_flag(self, monkeypatch):
        import cng_datasets.k8s.workflows as wf
        monkeypatch.setattr(wf, "_count_source_features", lambda *a, **k: 5000)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="trim-ds",
                source_url="https://example.com/wdpa.gdb",
                bucket="test-bucket",
                output_dir=tmpdir,
                h3_resolution=10,           # skip network geometry detection
                trim_strings=True,
            )
            convert_yaml = yaml.safe_load(open(Path(tmpdir) / "trim-ds-convert.yaml"))
            cmd = str(convert_yaml["spec"]["template"]["spec"]["containers"][0]["command"])
            assert "--trim-strings" in cmd

    def test_convert_job_omits_flag_by_default(self, monkeypatch):
        import cng_datasets.k8s.workflows as wf
        monkeypatch.setattr(wf, "_count_source_features", lambda *a, **k: 5000)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="notrim-ds",
                source_url="https://example.com/wdpa.gdb",
                bucket="test-bucket",
                output_dir=tmpdir,
                h3_resolution=10,
            )
            convert_yaml = yaml.safe_load(open(Path(tmpdir) / "notrim-ds-convert.yaml"))
            cmd = str(convert_yaml["spec"]["template"]["spec"]["containers"][0]["command"])
            assert "--trim-strings" not in cmd


class TestStepManifestNamespace:
    """
    Every generated step manifest carries metadata.namespace (issue #190).

    The docs sanction applying a step manifest on its own for step-by-step
    control, and the orchestrator passes `-n <namespace>`. A manifest without
    the field targets kubectl's default namespace instead, which fails with an
    RBAC error naming "default" rather than the missing field — or, on a
    permissive cluster, silently runs the job in the wrong namespace.
    """

    NAMESPACE = "geo-workflows"

    def _manifest_namespaces(self, tmpdir):
        found = {}
        for path in sorted(Path(tmpdir).glob("*.yaml")):
            # workflow-rbac.yaml holds several documents.
            for doc in yaml.safe_load_all(open(path)):
                if doc and doc.get("kind") == "Job":
                    found[path.name] = doc["metadata"].get("namespace")
        return found

    @pytest.mark.timeout(10)
    def test_raster_step_manifests_are_namespaced(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="ns-raster",
                source_urls=["https://example.com/a.tif", "https://example.com/b.tif"],
                bucket="test-bucket",
                namespace=self.NAMESPACE,
                output_dir=tmpdir,
            )
            found = self._manifest_namespaces(tmpdir)
            expected = {
                "ns-raster-setup-bucket.yaml",
                "ns-raster-preprocess-cog.yaml",
                "ns-raster-hex.yaml",
                "workflow.yaml",   # the orchestrator, which already had it
            }
            assert set(found) == expected, found
            assert all(ns == self.NAMESPACE for ns in found.values()), found

    @pytest.mark.timeout(10)
    def test_vector_step_manifests_are_namespaced(self, monkeypatch):
        """The gap was filed against raster, but the vector jobs had it too."""
        import cng_datasets.k8s.workflows as wf
        monkeypatch.setattr(wf, "_count_source_features", lambda *a, **k: 5000)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="ns-vector",
                source_url="https://example.com/wdpa.gdb",
                bucket="test-bucket",
                namespace=self.NAMESPACE,
                output_dir=tmpdir,
                h3_resolution=10,
            )
            found = self._manifest_namespaces(tmpdir)
            expected = {
                "ns-vector-setup-bucket.yaml",
                "ns-vector-convert.yaml",
                "ns-vector-pmtiles.yaml",
                "ns-vector-hex.yaml",
                "ns-vector-repartition.yaml",
                "workflow.yaml",   # the orchestrator, which already had it
            }
            assert set(found) == expected, found
            assert all(ns == self.NAMESPACE for ns in found.values()), found


class TestRasterHierarchicalDatasetPaths:
    """
    A hierarchical --dataset keeps its shape in S3 paths (issue #189).

    k8s object names must flatten 'a/b' to 'a-b', but the S3 path wants the
    original, as the vector generator already does via s3_dataset.
    """

    DATASET = "seafloor-carbon-flux/avg"

    def _hex_command(self, tmpdir):
        job = yaml.safe_load(open(Path(tmpdir) / "seafloor-carbon-flux-avg-hex.yaml"))
        return job["spec"]["template"]["spec"]["containers"][0]["command"][2]

    @pytest.mark.timeout(5)
    def test_hex_output_keeps_the_dataset_prefix(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name=self.DATASET,
                source_urls="https://example.com/x-cog.tif",
                bucket="public-high-seas",
                output_dir=tmpdir,
            )
            cmd = self._hex_command(tmpdir)
            assert "--output-parquet s3://public-high-seas/seafloor-carbon-flux/avg/hex/" in cmd
            assert "seafloor-carbon-flux-avg/hex/" not in cmd

    @pytest.mark.timeout(5)
    def test_k8s_object_names_still_flatten(self):
        """The S3 fix must not leak a '/' into a Kubernetes resource name."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name=self.DATASET,
                source_urls="https://example.com/x-cog.tif",
                bucket="public-high-seas",
                output_dir=tmpdir,
            )
            job = yaml.safe_load(open(Path(tmpdir) / "seafloor-carbon-flux-avg-hex.yaml"))
            assert job["metadata"]["name"] == "seafloor-carbon-flux-avg-hex"

    @pytest.mark.timeout(10)
    def test_preprocess_cog_lands_beside_the_dataset(self):
        """The COG mirrors the vector convention: '{dataset}-cog.tif'."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name=self.DATASET,
                source_urls=["https://example.com/a.tif", "https://example.com/b.tif"],
                bucket="public-high-seas",
                output_dir=tmpdir,
            )
            job = yaml.safe_load(
                open(Path(tmpdir) / "seafloor-carbon-flux-avg-preprocess-cog.yaml")
            )
            cmd = str(job["spec"]["template"]["spec"]["containers"][0]["command"])
            assert "s3://public-high-seas/seafloor-carbon-flux/avg-cog.tif" in cmd

    @pytest.mark.timeout(5)
    def test_flat_dataset_paths_are_unchanged(self):
        """A flat name has no hierarchy to preserve; its paths must not move."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="flat-raster",
                source_urls="https://example.com/x-cog.tif",
                bucket="test-bucket",
                output_dir=tmpdir,
            )
            job = yaml.safe_load(open(Path(tmpdir) / "flat-raster-hex.yaml"))
            cmd = job["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert "--output-parquet s3://test-bucket/flat-raster/hex/" in cmd


class TestH0Subset:
    """
    --h0-subset right-sizes the hex fan-out (issue #191).

    Without it every raster starts 122 pods, one per h0 base cell, so a CONUS
    source runs 116 pods that localize a multi-GB COG, find no overlap and exit.
    """

    CONUS = [12, 14, 20, 50, 71, 78]

    def _hex_job(self, tmpdir, **kwargs):
        generate_raster_workflow(
            dataset_name="h0-demo",
            source_urls="https://example.com/x-cog.tif",
            bucket="test-bucket",
            output_dir=tmpdir,
            **kwargs,
        )
        return yaml.safe_load(open(Path(tmpdir) / "h0-demo-hex.yaml"))

    @pytest.mark.timeout(5)
    def test_default_still_covers_every_base_cell(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            job = self._hex_job(tmpdir)
            assert job["spec"]["completions"] == 122
            cmd = job["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert "--h0-index ${JOB_COMPLETION_INDEX}" in cmd
            assert "H0S=(" not in cmd

    @pytest.mark.timeout(5)
    def test_subset_sets_completions_and_index_mapping(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            job = self._hex_job(tmpdir, h0_subset=self.CONUS)
            assert job["spec"]["completions"] == 6
            cmd = job["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert "H0S=(12 14 20 50 71 78)" in cmd
            assert "H0=${H0S[$JOB_COMPLETION_INDEX]}" in cmd
            assert "--h0-index ${H0}" in cmd

    @pytest.mark.timeout(5)
    def test_parallelism_does_not_exceed_completions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            job = self._hex_job(tmpdir, h0_subset=self.CONUS, max_parallelism=61)
            assert job["spec"]["parallelism"] == 6

    @pytest.mark.timeout(5)
    def test_subset_is_sorted_and_deduplicated(self):
        """The emitted list must be stable so an index maps to the same cell."""
        with tempfile.TemporaryDirectory() as tmpdir:
            job = self._hex_job(tmpdir, h0_subset=[50, 12, 50, 14])
            cmd = job["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert "H0S=(12 14 50)" in cmd
            assert job["spec"]["completions"] == 3

    @pytest.mark.timeout(5)
    def test_full_subset_collapses_to_the_default_fan_out(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            job = self._hex_job(tmpdir, h0_subset=list(range(122)))
            assert job["spec"]["completions"] == 122
            cmd = job["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert "H0S=(" not in cmd

    @pytest.mark.timeout(5)
    def test_out_of_range_cell_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="0-121"):
                self._hex_job(tmpdir, h0_subset=[12, 122])

    @pytest.mark.timeout(5)
    def test_empty_subset_is_rejected(self):
        """Silently falling back to 122 would hide a mis-parsed flag."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="empty"):
                self._hex_job(tmpdir, h0_subset=[])

    @pytest.mark.timeout(5)
    def test_index_mapping_resolves_the_right_cell_in_bash(self):
        """The emitted preamble is bash — run it rather than trust the string."""
        import subprocess
        with tempfile.TemporaryDirectory() as tmpdir:
            job = self._hex_job(tmpdir, h0_subset=self.CONUS)
            script = job["spec"]["template"]["spec"]["containers"][0]["command"][2]
            script = script.replace("cng-datasets raster", "echo RAN:")
            run = subprocess.run(
                ["bash", "-c", script],
                capture_output=True, text=True,
                env={"PATH": "/usr/bin:/bin", "JOB_COMPLETION_INDEX": "3"},
            )
            assert run.returncode == 0, run.stderr
            assert "--h0-index 50" in run.stdout

    @pytest.mark.timeout(5)
    def test_index_past_the_end_fails_loudly(self):
        """An out-of-range index must not silently process cell 0 of the list."""
        import subprocess
        with tempfile.TemporaryDirectory() as tmpdir:
            job = self._hex_job(tmpdir, h0_subset=self.CONUS)
            script = job["spec"]["template"]["spec"]["containers"][0]["command"][2]
            script = script.replace("cng-datasets raster", "echo RAN:")
            for index in ("9", ""):
                run = subprocess.run(
                    ["bash", "-c", script],
                    capture_output=True, text=True,
                    env={"PATH": "/usr/bin:/bin", "JOB_COMPLETION_INDEX": index},
                )
                assert run.returncode == 1, f"index {index!r}: {run.stdout}"
                assert "No h0 cell for completion index" in run.stderr
                assert "RAN:" not in run.stdout


class TestCsvAndCountFlagsReachTheConvertStep:
    """
    A CSV point source needs its lat/lon columns named on `workflow` (issue
    #188), and the convert step can be gated on an expected count (issue #186).

    Without the former a CSV source could not be a --source-url for a generated
    pipeline at all; it had to be pre-converted in a separate job.
    """

    def _convert_command(self, tmpdir, monkeypatch, **kwargs):
        import cng_datasets.k8s.workflows as wf
        monkeypatch.setattr(wf, "_count_source_features", lambda *a, **k: 5000)
        generate_dataset_workflow(
            dataset_name="ics209",
            source_url="https://example.com/incidents.csv",
            bucket="public-fire",
            output_dir=tmpdir,
            h3_resolution=10,
            **kwargs,
        )
        job = yaml.safe_load(open(Path(tmpdir) / "ics209-convert.yaml"))
        return job["spec"]["template"]["spec"]["containers"][0]["command"][2]

    @pytest.mark.timeout(10)
    def test_latlon_columns_are_passed_through(self, monkeypatch):
        with tempfile.TemporaryDirectory() as tmpdir:
            cmd = self._convert_command(
                tmpdir, monkeypatch,
                lat_column="POO_LATITUDE", lon_column="POO_LONGITUDE",
            )
            assert "--lat-column POO_LATITUDE" in cmd
            assert "--lon-column POO_LONGITUDE" in cmd

    @pytest.mark.timeout(10)
    def test_expect_features_is_passed_through(self, monkeypatch):
        with tempfile.TemporaryDirectory() as tmpdir:
            cmd = self._convert_command(tmpdir, monkeypatch, expect_features=127133)
            assert "--expect-features 127133" in cmd

    @pytest.mark.timeout(10)
    def test_flags_are_absent_by_default(self, monkeypatch):
        with tempfile.TemporaryDirectory() as tmpdir:
            cmd = self._convert_command(tmpdir, monkeypatch)
            for flag in ("--lat-column", "--lon-column", "--expect-features"):
                assert flag not in cmd


class TestHexWorkerSizing:
    """
    The hex pod's worker count is pinned in the manifest (issue #195).

    Left unset, `cng-datasets raster` sizes its pool from the cgroup CPU quota
    and falls back to the node's core count when that is unreadable — so one
    manifest produced 48 workers on one node and 64 on another, on a pod that
    requested 4 CPUs, and peak RSS followed. Emitting CNG_HEX_WORKERS and
    CNG_HEX_CHUNK_SIZE makes the pod's memory profile a property of the
    manifest, and makes a tuned value survive regeneration.
    """

    def _hex_container(self, tmpdir, monkeypatch, **kwargs):
        import cng_datasets.raster.cog as cog
        monkeypatch.setattr(cog, "is_cog", lambda *a, **k: True)
        generate_raster_workflow(
            dataset_name="hexdemo",
            source_urls="https://example.com/x.tif",
            bucket="test-bucket",
            output_dir=tmpdir,
            **kwargs,
        )
        job = yaml.safe_load(open(Path(tmpdir) / "hexdemo-hex.yaml"))
        return job["spec"]["template"]["spec"]["containers"][0]

    @staticmethod
    def _env(container, name):
        for entry in container["env"]:
            if entry["name"] == name:
                return entry["value"]
        return None

    @pytest.mark.timeout(5)
    def test_defaults_are_pinned_not_inherited(self, monkeypatch):
        """Even an untuned manifest states both halves of the peak-RSS product."""
        with tempfile.TemporaryDirectory() as tmpdir:
            c = self._hex_container(tmpdir, monkeypatch)
            # One worker per requested CPU — not the 48/64 the runtime inferred.
            assert self._env(c, "CNG_HEX_WORKERS") == "4"
            assert self._env(c, "CNG_HEX_CHUNK_SIZE") == "100000"
            assert c["resources"]["requests"]["cpu"] == "4"

    @pytest.mark.timeout(5)
    def test_explicit_workers_override_the_cpu_default(self, monkeypatch):
        with tempfile.TemporaryDirectory() as tmpdir:
            c = self._hex_container(tmpdir, monkeypatch, hex_workers=8)
            assert self._env(c, "CNG_HEX_WORKERS") == "8"
            assert c["resources"]["requests"]["cpu"] == "4"

    @pytest.mark.timeout(5)
    def test_hex_cpu_sets_the_request_and_the_worker_default(self, monkeypatch):
        """The #590 hand-rolled shape — cpu 8, 8 workers — is now generatable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            c = self._hex_container(tmpdir, monkeypatch, hex_cpu="8")
            assert self._env(c, "CNG_HEX_WORKERS") == "8"
            assert c["resources"]["requests"]["cpu"] == "8"
            assert c["resources"]["limits"]["cpu"] == "8"

    @pytest.mark.timeout(5)
    def test_millicpu_request_floors_to_whole_workers(self, monkeypatch):
        """A fractional CPU request still has to yield at least one worker."""
        with tempfile.TemporaryDirectory() as tmpdir:
            c = self._hex_container(tmpdir, monkeypatch, hex_cpu="3500m")
            assert self._env(c, "CNG_HEX_WORKERS") == "3"
            assert c["resources"]["requests"]["cpu"] == "3500m"
        with tempfile.TemporaryDirectory() as tmpdir:
            c = self._hex_container(tmpdir, monkeypatch, hex_cpu="500m")
            assert self._env(c, "CNG_HEX_WORKERS") == "1"

    @pytest.mark.timeout(5)
    def test_chunk_size_is_emitted(self, monkeypatch):
        with tempfile.TemporaryDirectory() as tmpdir:
            c = self._hex_container(tmpdir, monkeypatch, hex_chunk_size=25000)
            assert self._env(c, "CNG_HEX_CHUNK_SIZE") == "25000"

    @pytest.mark.timeout(5)
    @pytest.mark.parametrize("kwargs", [
        {"hex_workers": 0},
        {"hex_workers": -1},
        {"hex_chunk_size": 0},
        {"hex_cpu": "0"},
        {"hex_cpu": "four"},
    ])
    def test_unusable_sizing_is_rejected_at_generation(self, monkeypatch, kwargs):
        """A knob that cannot produce a working pod fails before the YAML is written."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError):
                self._hex_container(tmpdir, monkeypatch, **kwargs)

    @pytest.mark.timeout(5)
    def test_rejection_leaves_no_partial_workflow(self, monkeypatch):
        """The check runs before the first manifest, not between two of them."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError):
                self._hex_container(tmpdir, monkeypatch, hex_workers=0)
            assert list(Path(tmpdir).iterdir()) == []

    @pytest.mark.timeout(10)
    def test_armada_conversion_preserves_the_worker_count(self, monkeypatch):
        """--backend armada must not drop the lever (issue #183 is about what it does drop)."""
        import cng_datasets.raster.cog as cog
        monkeypatch.setattr(cog, "is_cog", lambda *a, **k: True)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="hexdemo",
                source_urls="https://example.com/x.tif",
                bucket="test-bucket",
                output_dir=tmpdir,
                backend="armada",
                hex_workers=8,
                hex_chunk_size=25000,
            )
            text = (Path(tmpdir) / "armada-hexdemo-hex.yaml").read_text()
            assert "CNG_HEX_WORKERS" in text
            assert "CNG_HEX_CHUNK_SIZE" in text

    @pytest.mark.timeout(5)
    def test_vector_hex_job_does_not_advertise_the_knob(self, monkeypatch):
        """
        Only the raster hex step runs a worker pool.

        `cng-datasets vector` never reads CNG_HEX_WORKERS, so emitting it on the
        vector hex job would be a knob that silently does nothing — the exact
        failure mode #195 exists to remove.
        """
        import cng_datasets.k8s.workflows as wf
        monkeypatch.setattr(wf, "_count_source_features", lambda *a, **k: 5000)
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="vec",
                source_url="https://example.com/x.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
            )
            job = yaml.safe_load(open(Path(tmpdir) / "vec-hex.yaml"))
            env = job["spec"]["template"]["spec"]["containers"][0]["env"]
            assert not [e for e in env if e["name"].startswith("CNG_HEX")]


class TestChunkResolutionGeneration:
    """
    raster-workflow emits sub-h0 chunking end to end (issue #173).

    The engine gained --chunk-resolution in #200 but nothing emitted it, so the
    memory win was unreachable from generated output — the same gap #195 closed
    for CNG_HEX_WORKERS. These tests cover the three things a generated
    sub-h0 build must get right: a fan-out that matches the chunk list exactly,
    a merge step that actually runs, and staging that keeps parts out of the
    published tree until it does.
    """

    H0 = 577199624117288959

    @pytest.fixture(autouse=True)
    def _offline(self, monkeypatch):
        """Chunk enumeration and COG detection without network."""
        import duckdb
        import cng_datasets.raster.cog as cog
        monkeypatch.setattr(cog, "is_cog", lambda *a, **k: True)

        def fake_enum(chunk_resolution, h0_subset=None, h0_grid_path=None, con=None):
            if h0_subset is not None and 0 not in h0_subset:
                return []
            if chunk_resolution == 0:
                return [(self.H0, self.H0, 0)]
            c = duckdb.connect()
            c.execute("INSTALL h3 FROM community; LOAD h3;")
            kids = [r[0] for r in c.execute(
                f"SELECT UNNEST(h3_cell_to_children({self.H0}, {chunk_resolution}))"
            ).fetchall()]
            return sorted(((int(k), self.H0, 0) for k in kids),
                          key=lambda r: (r[2], r[0]))

        monkeypatch.setattr(cog, "enumerate_chunk_cells", fake_enum)
        self._enum = fake_enum

    def _build(self, tmpdir, **kwargs):
        generate_raster_workflow(
            dataset_name="chunky",
            source_urls="https://example.com/x.tif",
            bucket="test-bucket",
            output_dir=tmpdir,
            h3_resolution=10,
            **kwargs,
        )
        return Path(tmpdir)

    def _load(self, path):
        with open(path) as f:
            return yaml.safe_load(f)

    @pytest.mark.timeout(60)
    def test_completions_match_the_chunk_list_exactly(self):
        """
        A fan-out narrower than the chunk list drops chunks with a clean exit.

        Generator and pod must agree, which is why both go through
        enumerate_chunk_cells rather than each counting for themselves.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            out = self._build(tmpdir, chunk_resolution=2)
            job = self._load(out / "chunky-hex.yaml")
            assert job["spec"]["completions"] == len(self._enum(2))
            assert job["spec"]["parallelism"] <= job["spec"]["completions"]

    @pytest.mark.timeout(60)
    def test_hex_command_carries_the_chunk_selector(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out = self._build(tmpdir, chunk_resolution=2)
            cmd = self._load(out / "chunky-hex.yaml")["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert "--chunk-resolution 2" in cmd
            assert "--chunk-index ${JOB_COMPLETION_INDEX}" in cmd
            assert "--h0-index" not in cmd

    @pytest.mark.timeout(60)
    def test_chunks_are_staged_outside_the_published_tree(self):
        """
        A reader globbing hex/ mid-build must never see half-merged parts.

        h0={cell}/data_0.parquet is a literal path in published STAC READMEs,
        so parts land in hex-chunks/ and only the merge writes hex/.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            out = self._build(tmpdir, chunk_resolution=2)
            cmd = self._load(out / "chunky-hex.yaml")["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert "s3://test-bucket/chunky/hex-chunks/" in cmd
            assert "--output-parquet s3://test-bucket/chunky/hex/" not in cmd

            merge = self._load(out / "chunky-merge.yaml")
            mcmd = " ".join(merge["spec"]["template"]["spec"]["containers"][0]["command"][2].split())
            assert "--chunks-dir s3://test-bucket/chunky/hex-chunks" in mcmd
            assert "--output-dir s3://test-bucket/chunky/hex" in mcmd

    @pytest.mark.timeout(60)
    def test_merge_reaches_the_orchestrator_and_the_configmap(self):
        """
        The orchestrator applies manifests from /yamls, i.e. from the ConfigMap.

        Emitting chunky-merge.yaml but leaving it out of the ConfigMap fails the
        build at the merge step, long after generation looked fine.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            out = self._build(tmpdir, chunk_resolution=2)
            cm = self._load(out / "configmap.yaml")
            assert "chunky-merge.yaml" in cm["data"]
            args = self._load(out / "workflow.yaml")["spec"]["template"]["spec"]["containers"][0]["args"][0]
            assert "chunky-merge.yaml" in args
            assert args.index("chunky-hex.yaml") < args.index("chunky-merge.yaml")

    @pytest.mark.timeout(60)
    def test_merge_is_told_how_many_chunks_to_expect(self):
        """
        The generator sized the fan-out, so it is the only place that knows.

        Without this the merge consolidates whatever survived a partly failed
        fan-out and publishes it as complete — and then deletes the chunks that
        would have shown which were missing.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            out = self._build(tmpdir, chunk_resolution=2)
            expected = self._load(out / "chunky-hex.yaml")["spec"]["completions"]
            mcmd = " ".join(
                self._load(out / "chunky-merge.yaml")["spec"]["template"]["spec"]
                ["containers"][0]["command"][2].split())
            assert f"--expect-chunks {expected}" in mcmd

    @pytest.mark.timeout(60)
    def test_no_merge_step_without_sub_chunking(self):
        """The default path gains nothing and must stay exactly as it was."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out = self._build(tmpdir)
            assert not (out / "chunky-merge.yaml").exists()
            cm = self._load(out / "configmap.yaml")
            assert "chunky-merge.yaml" not in cm["data"]
            cmd = self._load(out / "chunky-hex.yaml")["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert "--h0-index" in cmd
            assert "--chunk-resolution" not in cmd
            assert "s3://test-bucket/chunky/hex/" in cmd

    @pytest.mark.timeout(60)
    def test_h0_subset_is_passed_through_for_sub_chunks(self):
        """The pod re-derives the chunk list, so it needs the same subset."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out = self._build(tmpdir, chunk_resolution=1, h0_subset=[0])
            cmd = self._load(out / "chunky-hex.yaml")["spec"]["template"]["spec"]["containers"][0]["command"][2]
            assert '--h0-subset "0"' in cmd
            # The bash H0S index mapping belongs to the res-0 fan-out only.
            assert "H0S=(" not in cmd


class TestChunkResolutionBudget:
    """
    --max-hex-memory picks a chunk resolution from the #173 measurements.

    The model is deliberately simple — cells per chunk times a measured
    bytes-per-cell — because it only has to be right enough to pick a level, and
    a model nobody can check is worse than a rough one they can.
    """

    @pytest.mark.timeout(30)
    @pytest.mark.parametrize("chunk_res,measured_gib", [(0, 32.0), (1, 4.6), (2, 0.68)])
    def test_model_reproduces_the_measured_points(self, chunk_res, measured_gib):
        """The constant is only credible if it returns the numbers it came from."""
        from cng_datasets.k8s.workflows import estimate_chunk_peak_bytes
        est_gib = estimate_chunk_peak_bytes(10, chunk_res) / 2 ** 30
        assert abs(est_gib - measured_gib) / measured_gib < 0.10, (
            f"model says {est_gib:.2f} GiB at chunk res {chunk_res}, "
            f"measurement in #173 says {measured_gib} GiB"
        )

    @pytest.mark.timeout(30)
    def test_picks_the_coarsest_resolution_that_fits(self):
        """
        Coarsest, not finest: every extra level multiplies the pod count ~7x.

        Picking the smallest chunk that fits would be defensible for memory
        alone and badly wrong for everything else — scheduling, image pulls, and
        the source reads each pod does.
        """
        from cng_datasets.k8s.workflows import select_chunk_resolution, estimate_chunk_peak_bytes
        for budget, expected in [("32Gi", 0), ("8Gi", 1), ("1Gi", 2)]:
            chosen = select_chunk_resolution(10, budget)
            assert chosen == expected, f"{budget} chose res {chosen}, expected {expected}"
            if chosen > 0:
                # the next level coarser must genuinely not fit
                assert estimate_chunk_peak_bytes(10, chosen - 1) > _parse(budget)

    @pytest.mark.timeout(30)
    def test_impossible_budget_says_so(self):
        """Only a budget below one cell's cost is truly unsatisfiable."""
        from cng_datasets.k8s.workflows import select_chunk_resolution
        with pytest.raises(ValueError, match="No chunk resolution fits"):
            select_chunk_resolution(10, "100")

    @pytest.mark.timeout(30)
    def test_a_tiny_budget_is_caught_by_the_fan_out_ceiling(self):
        """
        A budget the model can satisfy can still be a fan-out nobody wants.

        1Ki fits seven native cells per chunk, so the model happily chooses
        res 9 — tens of millions of pods, and a chunk list that hangs generation
        before a single manifest is written.
        """
        import tempfile as _tf
        from cng_datasets.k8s.workflows import select_chunk_resolution
        assert select_chunk_resolution(10, "1Ki") >= 9
        with _tf.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="ceiling"):
                generate_raster_workflow(
                    dataset_name="huge",
                    source_urls="https://example.com/x.tif",
                    bucket="b", output_dir=tmpdir,
                    h3_resolution=10, max_hex_memory="1Ki",
                )

    @pytest.mark.timeout(30)
    def test_memory_quantities_parse(self):
        from cng_datasets.k8s.workflows import _parse_memory_to_bytes as p
        assert p("32Gi") == 32 * 2 ** 30
        assert p("512Mi") == 512 * 2 ** 20
        assert p("1G") == 10 ** 9
        assert p("1000") == 1000

    @pytest.mark.timeout(30)
    def test_chunk_finer_than_target_is_rejected(self):
        from cng_datasets.k8s.workflows import estimate_chunk_peak_bytes
        with pytest.raises(ValueError, match="finer than"):
            estimate_chunk_peak_bytes(8, 9)


def _parse(q):
    from cng_datasets.k8s.workflows import _parse_memory_to_bytes
    return _parse_memory_to_bytes(q)


class TestChunkBackendRouting:
    """
    Fine chunking is what makes an external queue necessary (#173 lever D, #39).

    An indexed Job of thousands of completions is past the namespace pod
    guideline, which is the same problem #183's finding 1 describes from the
    scheduler side.
    """

    H0 = 577199624117288959

    @pytest.fixture(autouse=True)
    def _offline(self, monkeypatch):
        import duckdb
        import cng_datasets.raster.cog as cog
        monkeypatch.setattr(cog, "is_cog", lambda *a, **k: True)

        def fake_enum(chunk_resolution, h0_subset=None, h0_grid_path=None, con=None):
            if chunk_resolution == 0:
                return [(self.H0, self.H0, 0)]
            c = duckdb.connect()
            c.execute("INSTALL h3 FROM community; LOAD h3;")
            kids = [r[0] for r in c.execute(
                f"SELECT UNNEST(h3_cell_to_children({self.H0}, {chunk_resolution}))"
            ).fetchall()]
            return sorted(((int(k), self.H0, 0) for k in kids),
                          key=lambda r: (r[2], r[0]))
        monkeypatch.setattr(cog, "enumerate_chunk_cells", fake_enum)

    def _build(self, tmpdir, **kwargs):
        generate_raster_workflow(
            dataset_name="routed",
            source_urls="https://example.com/x.tif",
            bucket="test-bucket",
            output_dir=tmpdir,
            h3_resolution=10,
            **kwargs,
        )
        return Path(tmpdir)

    @pytest.mark.timeout(60)
    def test_auto_stays_on_k8s_below_the_guideline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out = self._build(tmpdir, chunk_resolution=2, backend="auto")  # 49 chunks
            assert (out / "workflow.yaml").exists()
            assert not list(out.glob("armada-*.yaml"))

    @pytest.mark.timeout(120)
    def test_auto_routes_to_armada_above_the_guideline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            out = self._build(tmpdir, chunk_resolution=3, backend="auto")  # 343 chunks
            assert list(out.glob("armada-*.yaml")), "343 chunks should route to Armada"
            assert (out / "armada-routed-merge.yaml").exists(), "merge must convert too"

    @pytest.mark.timeout(120)
    def test_explicit_k8s_is_warned_not_overridden(self, capsys):
        """
        An explicit backend is the operator's call; say the cost, do not override.

        Silently switching what someone asked for is the same class of surprise
        the rest of this work exists to remove.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            out = self._build(tmpdir, chunk_resolution=3, backend="k8s")
            assert not list(out.glob("armada-*.yaml"))
            printed = capsys.readouterr().out
            assert "343 chunks exceeds" in printed
            assert "--backend armada" in printed

    @pytest.mark.timeout(60)
    def test_budget_and_explicit_resolution_are_mutually_exclusive(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="both given"):
                self._build(tmpdir, chunk_resolution=2, max_hex_memory="8Gi")

    @pytest.mark.timeout(60)
    def test_chunk_finer_than_target_resolution_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="finer than"):
                self._build(tmpdir, chunk_resolution=11)


class TestHexFanOutSchedulingSafety:
    """
    A long hex fan-out must not be lowest-priority and must not be retry-less
    (issue #201).

    Two separate defects that happened to land on the same job. `opportunistic`
    is NRP's lowest priority (-2000000000) and preemption exposure scales with
    runtime, so it was applied to exactly the pods least able to absorb it —
    multi-hour, un-checkpointed, restarting from zero. And `backoffLimit: 0` is
    a *job-wide* budget: one failure anywhere killed the whole fan-out.
    """

    @pytest.fixture(autouse=True)
    def _offline(self, monkeypatch):
        import cng_datasets.raster.cog as cog
        monkeypatch.setattr(cog, "is_cog", lambda *a, **k: True)

    def _raster_hex(self, tmpdir, **kwargs):
        generate_raster_workflow(
            dataset_name="sched", source_urls="https://example.com/x.tif",
            bucket="test-bucket", output_dir=tmpdir, **kwargs)
        with open(Path(tmpdir) / "sched-hex.yaml") as f:
            return yaml.safe_load(f)

    def _vector_hex(self, tmpdir, monkeypatch, **kwargs):
        import cng_datasets.k8s.workflows as wf
        monkeypatch.setattr(wf, "_count_source_features", lambda *a, **k: 5000)
        generate_dataset_workflow(
            dataset_name="vsched", source_url="https://example.com/x.gpkg",
            bucket="test-bucket", output_dir=tmpdir, **kwargs)
        with open(Path(tmpdir) / "vsched-hex.yaml") as f:
            return yaml.safe_load(f)

    @pytest.mark.timeout(60)
    def test_hex_pods_are_not_lowest_priority_by_default(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pod = self._raster_hex(tmpdir)["spec"]["template"]["spec"]
            assert "priorityClassName" not in pod, (
                "hex pods must default to default priority; opportunistic is "
                "NRP's lowest and preemption exposure scales with runtime"
            )

    @pytest.mark.timeout(60)
    def test_nrp_profile_does_not_reimpose_opportunistic(self):
        """
        The profile is where the measured build got its priority from.

        Fixing the dataclass default but leaving the profile would fix nothing
        for anyone actually passing --profile nrp.
        """
        from cng_datasets.k8s import load_profile
        assert not load_profile("nrp").get("priority_class")

    @pytest.mark.timeout(60)
    def test_opportunistic_is_still_reachable(self):
        """It remains right for genuinely interruptible work, and for quota."""
        with tempfile.TemporaryDirectory() as tmpdir:
            pod = self._raster_hex(tmpdir, priority_class="opportunistic")["spec"]["template"]["spec"]
            assert pod["priorityClassName"] == "opportunistic"

    @pytest.mark.timeout(60)
    def test_fan_out_has_a_per_index_retry_budget(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            spec = self._raster_hex(tmpdir)["spec"]
            assert spec["backoffLimitPerIndex"] == 2
            assert spec["maxFailedIndexes"] == 1
            assert "backoffLimit" not in spec, (
                "a job-wide backoffLimit alongside a per-index budget is ignored "
                "by Kubernetes and misleads anyone reading the manifest"
            )

    @pytest.mark.timeout(60)
    def test_vector_fan_out_gets_the_same_treatment(self, monkeypatch):
        """
        Filed against raster-workflow; the vector generator had it too.

        Same shape, same fix — as with metadata.namespace in #190.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            spec = self._vector_hex(tmpdir, monkeypatch)["spec"]
            assert spec["backoffLimitPerIndex"] == 2
            assert spec["maxFailedIndexes"] == 1
            assert "backoffLimit" not in spec
            assert "priorityClassName" not in spec["template"]["spec"]

    @pytest.mark.timeout(60)
    def test_preemption_is_still_ignored_by_the_pod_failure_policy(self):
        """
        The half of #201 that was already handled must survive the fix.

        A preempted or drained pod sets DisruptionTarget; ignoring it means the
        index retries without spending its budget. Dropping that while adding
        retries would trade one gap for another.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            rules = self._raster_hex(tmpdir)["spec"]["podFailurePolicy"]["rules"]
            assert any(
                r["action"] == "Ignore"
                and any(c["type"] == "DisruptionTarget" for c in r["onPodConditions"])
                for r in rules
            )

    @pytest.mark.timeout(60)
    def test_merge_job_retries_rather_than_discarding_hours_of_hex(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="sched", source_urls="https://example.com/x.tif",
                bucket="test-bucket", output_dir=tmpdir, chunk_resolution=1)
            with open(Path(tmpdir) / "sched-merge.yaml") as f:
                spec = yaml.safe_load(f)["spec"]
            assert spec["backoffLimit"] >= 1

    @pytest.mark.timeout(60)
    def test_a_zero_tolerance_fan_out_is_rejected(self):
        """max_failed_indexes=0 would reinstate exactly what this replaces."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="backoffLimit: 0 behaviour"):
                self._raster_hex(tmpdir, max_failed_indexes=0)

    @pytest.mark.timeout(60)
    def test_retries_are_configurable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            spec = self._raster_hex(tmpdir, hex_retries=5, max_failed_indexes=3)["spec"]
            assert spec["backoffLimitPerIndex"] == 5
            assert spec["maxFailedIndexes"] == 3
