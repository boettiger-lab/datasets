# cng_datasets Test Suite

Comprehensive unit tests for the cng_datasets package.

## Running Tests

```bash
# Run all tests (with 10-second timeout per test)
pytest tests/ -v

# Run specific test file
pytest tests/test_k8s_workflows.py -v

# Run specific test
pytest tests/test_cli.py::TestCLI::test_workflow_command -v

# Run with coverage
pytest tests/ --cov=cng_datasets --cov-report=html

# Run only fast tests (exclude slow/integration tests)
pytest tests/ -v -m "not slow and not integration"
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
Unit tests for vector data processing including:
- DuckDB connection setup with H3 extension
- Geometry to H3 cell conversion
- H3 parent resolution calculation
- Polygon and point handling
- H3VectorProcessor initialization and chunk processing

**Note:** All S3/network integration tests have been removed or mocked to keep tests fast and reliable.

## Test Timeouts

All tests have a 10-second timeout configured globally in `pyproject.toml`. Tests that hang or take too long will automatically fail with a timeout error. Individual tests can override this with the `@pytest.mark.timeout()` decorator.

## Test Coverage

Currently: **All unit tests passing** with proper timeout protection.

Tests focus on:
- Core functionality with fast in-memory operations
- Mocked external dependencies (S3, network calls)
- Proper error handling and edge cases

## Dependencies

```bash
pip install pytest pyyaml
```

All other dependencies are already in the main package requirements.
