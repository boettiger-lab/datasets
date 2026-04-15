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
            # 5-feature fixture -> chunk_size=1, completions=5, parallelism=5
            assert job["spec"]["completions"] == 5
            assert job["spec"]["parallelism"] == 5
            assert job["spec"]["completionMode"] == "Indexed"
    
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
            # 5-feature fixture -> chunk_size=1, completions=5, parallelism=5
            assert job["spec"]["completions"] == 5
            assert job["spec"]["parallelism"] == 5
            assert job["spec"]["completionMode"] == "Indexed"

            # Check chunk-size is set correctly
            command = job["spec"]["template"]["spec"]["containers"][0]["command"]
            command_str = str(command)
            assert "--chunk-size 1" in command_str

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
        """Hex job command should include --hex-resampling with the default 'average'."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_raster_workflow(
                dataset_name="test-raster",
                source_urls=self.SOURCE_URL,
                bucket="test-bucket",
                output_dir=tmpdir,
            )
            hex_job = self._load_yaml(Path(tmpdir) / "test-raster-hex.yaml")
            command_str = str(hex_job["spec"]["template"]["spec"]["containers"][0]["command"])
            assert "--hex-resampling average" in command_str

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
        assert profile["priority_class"] == "opportunistic"
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
