"""
Unit tests for K8s workflow generation.
"""

import tempfile
import yaml
from pathlib import Path
import pytest

from cng_datasets.k8s import generate_dataset_workflow, K8sJobManager


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
                source_url="https://example.com/data.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir,
                namespace="test-ns"
            )
            
            output_path = Path(tmpdir)
            
            # Check all expected files exist
            expected_files = [
                "convert-job.yaml",
                "pmtiles-job.yaml",
                "hex-job.yaml",
                "repartition-job.yaml",
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
                source_url="https://example.com/data.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir
            )
            
            convert_file = Path(tmpdir) / "convert-job.yaml"
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
                source_url="https://example.com/data.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir
            )
            
            hex_file = Path(tmpdir) / "hex-job.yaml"
            with open(hex_file) as f:
                job = yaml.safe_load(f)
                
            assert job["metadata"]["name"] == "test-ds-hex"
            # With automatic chunking, when row count fails, we use defaults
            assert job["spec"]["completions"] == 200  # Default max completions
            assert job["spec"]["parallelism"] == 50   # Default max parallelism
            assert job["spec"]["completionMode"] == "Indexed"
    
    @pytest.mark.timeout(30)
    @pytest.mark.integration
    def test_hex_job_real_bucket(self):
        """Test hex job with real public bucket calculates proper chunking."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="mappinginequality",
                source_url="https://dsl.richmond.edu/panorama/redlining/static/mappinginequality.gpkg",
                bucket="public-mappinginequality",
                output_dir=tmpdir
            )
            
            hex_file = Path(tmpdir) / "hex-job.yaml"
            with open(hex_file) as f:
                job = yaml.safe_load(f)
                
            assert job["metadata"]["name"] == "mappinginequality-hex"
            # Should calculate based on actual row count (10,154 rows)
            # 10,154 / 200 = ~51 per chunk, so 200 completions
            assert job["spec"]["completions"] == 200
            assert job["spec"]["parallelism"] == 50
            assert job["spec"]["completionMode"] == "Indexed"
            
            # Check chunk-size is set correctly
            command = job["spec"]["template"]["spec"]["containers"][0]["command"]
            command_str = str(command)
            assert "--chunk-size 51" in command_str
    
    @pytest.mark.timeout(5)
    def test_workflow_rbac(self):
        """Test RBAC configuration is generated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://example.com/data.gpkg",
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


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    @pytest.mark.timeout(5)
    def test_invalid_dataset_name(self):
        """Test with dataset name containing special characters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Should handle this gracefully
            generate_dataset_workflow(
                dataset_name="my-dataset-2024",
                source_url="https://example.com/data.gpkg",
                bucket="test-bucket",
                output_dir=tmpdir
            )
            
            # Check files were created
            assert (Path(tmpdir) / "convert-job.yaml").exists()
    
    @pytest.mark.timeout(5)
    def test_output_dir_created(self):
        """Test that output directory is created if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "subdir" / "workflow"
            
            generate_dataset_workflow(
                dataset_name="test-ds",
                source_url="https://example.com/data.gpkg",
                bucket="test-bucket",
                output_dir=str(output_dir)
            )
            
            assert output_dir.exists()
            assert (output_dir / "convert-job.yaml").exists()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
