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
            assert (output_path / "test-ds-convert.yaml").exists()
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
                # geo-workflows is the site's namespace, supplied by the
                # profile so it changes in one place (was "biodiversity").
                assert docs[0]["metadata"]["namespace"] == "geo-workflows"
    
    @pytest.mark.timeout(5)
    def test_workflow_creates_expected_files(self):
        """Test that workflow command creates convert-job.yaml and workflow.yaml files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_args = [
                "cng-datasets",
                "workflow",
                "--dataset", "test-dataset",
                "--source-url", "https://example.com/data.gpkg",
                "--bucket", "test-bucket",
                "--output-dir", tmpdir
            ]
            
            with patch.object(sys, 'argv', test_args):
                main()
                
            # Verify expected files were created (matching CI test expectations)
            output_path = Path(tmpdir)
            assert (output_path / "test-dataset-convert.yaml").exists(), "convert-job.yaml not created"
            assert (output_path / "workflow.yaml").exists(), "workflow.yaml not created"



class TestCLIHelp:
    """Test CLI help commands."""
    
    @pytest.mark.timeout(5)
    def test_main_help(self):
        """Test main help command."""
        test_args = ["cng-datasets", "--help"]
        
        with patch.object(sys, 'argv', test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            # --help exits with 0
            assert exc_info.value.code == 0
    
    @pytest.mark.timeout(5)
    def test_workflow_help(self):
        """Test workflow subcommand help."""
        test_args = ["cng-datasets", "workflow", "--help"]
        
        with patch.object(sys, 'argv', test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0
    
    @pytest.mark.timeout(5)
    def test_k8s_help(self):
        """Test k8s subcommand help."""
        test_args = ["cng-datasets", "k8s", "--help"]
        
        with patch.object(sys, 'argv', test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0


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


class TestH0SubsetFlag:
    """--h0-subset must actually reach the generator (issue #191)."""

    @pytest.mark.timeout(10)
    def test_flag_parses_to_a_cell_list(self, tmp_path):
        captured = {}

        def fake_generate(**kwargs):
            captured.update(kwargs)

        test_args = [
            "cng-datasets", "raster-workflow",
            "--dataset", "cli-h0",
            "--source-url", "https://example.com/x-cog.tif",
            "--bucket", "test-bucket",
            "--h0-subset", "12,14, 20 ,50,71,78",
            "--output-dir", str(tmp_path),
        ]
        with patch("cng_datasets.k8s.generate_raster_workflow", fake_generate):
            with patch.object(sys, 'argv', test_args):
                main()

        assert captured["h0_subset"] == [12, 14, 20, 50, 71, 78]

    @pytest.mark.timeout(10)
    def test_omitting_the_flag_passes_none(self, tmp_path):
        captured = {}

        def fake_generate(**kwargs):
            captured.update(kwargs)

        test_args = [
            "cng-datasets", "raster-workflow",
            "--dataset", "cli-h0",
            "--source-url", "https://example.com/x-cog.tif",
            "--bucket", "test-bucket",
            "--output-dir", str(tmp_path),
        ]
        with patch("cng_datasets.k8s.generate_raster_workflow", fake_generate):
            with patch.object(sys, 'argv', test_args):
                main()

        assert captured["h0_subset"] is None


class TestH0CellsFlag:
    """
    --h0-cells takes H3 base cell numbers and resolves them (issue #213).

    --h0-subset takes positions in the h0 grid's ordering, but the obvious way
    to build a subset is to compute base cells with the H3 library — a list
    that --h0-subset accepts without complaint and resolves to entirely
    different cells. This is the flag that makes that list usable.
    """

    def _run(self, tmp_path, *flags, positions=None):
        captured = {}

        def fake_generate(**kwargs):
            captured.update(kwargs)

        def fake_resolve(base_cells, *a, **k):
            # The mapping is a property of the published grid, so the network
            # read is stubbed; tests/test_raster.py checks the resolution
            # itself against a fixture grid.
            assert base_cells == [9, 19, 20, 21, 34, 36]
            return positions

        test_args = [
            "cng-datasets", "raster-workflow",
            "--dataset", "cli-h0c",
            "--source-url", "https://example.com/x-cog.tif",
            "--bucket", "test-bucket",
            "--output-dir", str(tmp_path),
            *flags,
        ]
        with patch("cng_datasets.raster.cog.h0_positions_for_base_cells", fake_resolve):
            with patch("cng_datasets.k8s.generate_raster_workflow", fake_generate):
                with patch.object(sys, 'argv', test_args):
                    main()
        return captured

    @pytest.mark.timeout(10)
    def test_base_cells_reach_the_generator_as_positions(self, tmp_path):
        """The generator only ever sees positions, whichever flag was used."""
        captured = self._run(
            tmp_path, "--h0-cells", "9,19, 20 ,21,34,36",
            positions=[12, 14, 20, 50, 71, 78],
        )
        assert captured["h0_subset"] == [12, 14, 20, 50, 71, 78]

    @pytest.mark.timeout(10)
    def test_giving_both_flags_is_refused(self, tmp_path, capsys):
        """
        They are the same restriction in two numberings, and both lists are
        always in range — so a silent precedence rule would be exactly the
        hazard this flag exists to remove.
        """
        with pytest.raises(SystemExit) as exit_info:
            self._run(tmp_path, "--h0-subset", "12", "--h0-cells", "9",
                      positions=[12])
        assert exit_info.value.code == 1
        assert "pass one" in capsys.readouterr().err

    @pytest.mark.timeout(10)
    def test_the_raster_command_takes_it_too(self, tmp_path):
        """Both entry points, so a hand-run pod is not the odd one out."""
        captured = {}

        def fake_processor(**kwargs):
            captured.update(kwargs)
            raise SystemExit(0)

        test_args = [
            "cng-datasets", "raster",
            "--input", "https://example.com/x-cog.tif",
            "--output-parquet", str(tmp_path),
            "--h0-cells", "9,19,20,21,34,36",
        ]
        with patch("cng_datasets.raster.cog.h0_positions_for_base_cells",
                   lambda cells, *a, **k: [12, 14, 20, 50, 71, 78]):
            with patch("cng_datasets.raster.RasterProcessor", fake_processor):
                with patch.object(sys, 'argv', test_args):
                    with pytest.raises(SystemExit):
                        main()

        assert captured["h0_subset"] == [12, 14, 20, 50, 71, 78]


class TestHexSizingFlags:
    """--hex-workers and friends must reach the generator (issue #195)."""

    def _captured(self, tmp_path, *extra):
        captured = {}
        test_args = [
            "cng-datasets", "raster-workflow",
            "--dataset", "cli-hex",
            "--source-url", "https://example.com/x-cog.tif",
            "--bucket", "test-bucket",
            "--output-dir", str(tmp_path),
            *extra,
        ]
        with patch("cng_datasets.k8s.generate_raster_workflow",
                   lambda **kwargs: captured.update(kwargs)):
            with patch.object(sys, 'argv', test_args):
                main()
        return captured

    @pytest.mark.timeout(10)
    def test_flags_parse_and_reach_the_generator(self, tmp_path):
        captured = self._captured(
            tmp_path,
            "--hex-workers", "8", "--hex-cpu", "8", "--hex-chunk-size", "25000",
        )
        assert captured["hex_workers"] == 8
        assert captured["hex_cpu"] == "8"
        assert captured["hex_chunk_size"] == 25000

    @pytest.mark.timeout(10)
    def test_omitted_flags_leave_the_generator_defaults_alone(self, tmp_path):
        """
        Unset knobs are not forwarded at all.

        Passing None through would override the generator's own defaults with
        nothing, so the default worker count has to be decided in one place.
        """
        captured = self._captured(tmp_path)
        for key in ("hex_workers", "hex_cpu", "hex_chunk_size"):
            assert key not in captured
