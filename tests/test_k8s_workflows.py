"""
Unit tests for K8s workflow generation.
"""

import tempfile
import yaml
from pathlib import Path
import pytest

from cng_datasets.k8s import generate_dataset_workflow, K8sJobManager
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
            assert resources["requests"]["ephemeral-storage"] == "200Gi"
            assert resources["limits"]["ephemeral-storage"] == "200Gi"

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
            assert resources["requests"]["ephemeral-storage"] == "200Gi"

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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
