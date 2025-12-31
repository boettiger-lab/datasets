"""
Unit tests for CLI functionality.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import sys

from cng_datasets.cli import main


class TestCLI:
    """Test command-line interface."""
    
    @pytest.mark.timeout(5)
    def test_workflow_command(self):
        """Test workflow generation via CLI."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_args = [
                "cng-datasets",
                "workflow",
                "--dataset", "test-ds",
                "--source-url", "https://dsl.richmond.edu/panorama/redlining/static/mappinginequality.gpkg",
                "--bucket", "test-bucket",
                "--output-dir", tmpdir,
                "--namespace", "test-ns"
            ]
            
            with patch.object(sys, 'argv', test_args):
                # Should not raise any exceptions
                main()
                
            # Verify files were created
            output_path = Path(tmpdir)
            assert (output_path / "convert-job.yaml").exists()
            assert (output_path / "workflow.yaml").exists()
    
    @pytest.mark.timeout(5)
    def test_k8s_job_command(self):
        """Test single k8s job generation via CLI."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "test.yaml"
            
            # Use --cmd instead of --command to avoid conflict with subparser
            test_args = [
                "cng-datasets",
                "k8s",
                "--job-name", "test-job",
                "--output", str(output_file),
                "--namespace", "test-ns",
                "--cmd", "echo", "hello"
            ]
            
            with patch.object(sys, 'argv', test_args):
                main()
                
            assert output_file.exists()
    
    @pytest.mark.timeout(5)
    def test_chunked_k8s_job(self):
        """Test chunked k8s job generation via CLI."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "chunked.yaml"
            
            test_args = [
                "cng-datasets",
                "k8s",
                "--job-name", "chunked-job",
                "--output", str(output_file),
                "--chunks", "10",
                "--cmd", "python", "/app/script.py"
            ]
            
            with patch.object(sys, 'argv', test_args):
                main()
                
            assert output_file.exists()
            
            # Verify it's an indexed job
            import yaml
            with open(output_file) as f:
                job = yaml.safe_load(f)
                assert job["spec"]["completions"] == 10
                assert job["spec"]["completionMode"] == "Indexed"
    
    @pytest.mark.timeout(5)
    def test_no_command_shows_help(self):
        """Test that running with no command shows help."""
        test_args = ["cng-datasets"]
        
        with patch.object(sys, 'argv', test_args):
            with pytest.raises(SystemExit):
                main()
    
    @pytest.mark.timeout(5)
    def test_workflow_default_values(self):
        """Test workflow command with default values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_args = [
                "cng-datasets",
                "workflow",
                "--dataset", "test",
                "--source-url", "https://dsl.richmond.edu/panorama/redlining/static/mappinginequality.gpkg",
                "--bucket", "bucket",
                "--output-dir", tmpdir
                # namespace defaults to "biodiversity"
            ]
            
            with patch.object(sys, 'argv', test_args):
                main()
                
            # Check that files were created
            import yaml
            rbac_file = Path(tmpdir) / "workflow-rbac.yaml"
            with open(rbac_file) as f:
                docs = list(yaml.safe_load_all(f))
                # Check default namespace is used
                assert docs[0]["metadata"]["namespace"] == "biodiversity"


class TestCLIValidation:
    """Test CLI input validation."""
    
    @pytest.mark.timeout(5)
    def test_workflow_missing_required(self):
        """Test workflow command with missing required arguments."""
        test_args = [
            "cng-datasets",
            "workflow",
            "--dataset", "test"
            # Missing source-url and bucket
        ]
        
        with patch.object(sys, 'argv', test_args):
            with pytest.raises(SystemExit):
                main()
    
    @pytest.mark.timeout(5)
    def test_k8s_missing_required(self):
        """Test k8s command with missing required arguments."""
        test_args = [
            "cng-datasets",
            "k8s",
            "--job-name", "test"
            # Missing command
        ]
        
        with patch.object(sys, 'argv', test_args):
            with pytest.raises(SystemExit):
                main()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
