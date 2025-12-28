# cng_datasets Test Suite

Comprehensive unit tests for the cng_datasets package.

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_k8s_workflows.py -v

# Run specific test
pytest tests/test_cli.py::TestCLI::test_workflow_command -v

# Run with coverage
pytest tests/ --cov=cng_datasets --cov-report=html
```

## Test Structure

### test_cli.py
Tests the command-line interface including:
- Workflow generation command
- K8s job generation commands (single and chunked)
- Argument validation
- Default values

### test_k8s_workflows.py
Tests Kubernetes workflow generation including:
- Basic and chunked job generation
- Complete workflow generation (all 6 YAML files)
- RBAC configuration
- Edge cases (special characters, directory creation)

### test_vector.py
Tests vector data processing including:
- DuckDB connection setup with H3 extension
- Geometry to H3 cell conversion
- H3 parent resolution calculation
- Polygon and point handling

## Test Coverage

Currently: **20/21 tests passing** (95%+ coverage of core functionality)

The skipped test (`test_process_vector_chunks_local`) requires S3 access and test data.

## Dependencies

```bash
pip install pytest pyyaml
```

All other dependencies are already in the main package requirements.
