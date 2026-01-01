# Contributing to CNG Datasets

Thank you for your interest in contributing to the CNG Datasets toolkit! This document provides guidelines for contributing to the project.

## Development Setup

### Prerequisites

- Python 3.9 or higher
- Git
- Virtual environment tool (venv, conda, etc.)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/boettiger-lab/datasets.git
cd datasets
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install the package in development mode with dev dependencies:
```bash
pip install -e ".[dev]"
```

## Development Workflow

### Running Tests

Run the full test suite:
```bash
pytest tests/
```

Run specific test files:
```bash
pytest tests/test_vector.py -v
pytest tests/test_raster.py -v
```

Run with coverage:
```bash
pytest --cov=cng_datasets tests/
```

### Code Style

We use `black` for code formatting and `ruff` for linting.

Format code:
```bash
black cng_datasets/ tests/
```

Lint code:
```bash
ruff check cng_datasets/ tests/
```

Type checking (optional):
```bash
mypy cng_datasets/
```

### Making Changes

1. Create a new branch for your feature or bugfix:
```bash
git checkout -b feature/your-feature-name
```

2. Make your changes and add tests

3. Run tests and linters:
```bash
pytest tests/
black cng_datasets/ tests/
ruff check cng_datasets/ tests/
```

4. Commit your changes with a descriptive message:
```bash
git commit -m "Add feature: description of your changes"
```

5. Push to your fork and create a pull request

## Adding New Features

### Adding New Processing Functions

1. Add your function to the appropriate module:
   - Vector processing: `cng_datasets/vector/`
   - Raster processing: `cng_datasets/raster/`
   - Storage utilities: `cng_datasets/storage/`
   - Kubernetes jobs: `cng_datasets/k8s/`

2. Add comprehensive docstrings following the existing style

3. Add unit tests in `tests/test_*.py`

4. Update documentation in:
   - `README_PACKAGE.md` (user-facing docs)
   - Docstrings (API documentation)
   - Example notebooks/scripts if applicable

### Adding New CLI Commands

1. Add command definition in `cng_datasets/cli.py`

2. Add implementation in the appropriate module

3. Add tests for the new command

4. Update CLI documentation

## Testing Guidelines

### Unit Tests

- Test small, isolated units of functionality
- Use fixtures for test data setup
- Mock external services (S3, Kubernetes) when possible
- Aim for >80% code coverage

### Integration Tests

- Mark integration tests with `@pytest.mark.integration`
- Skip tests that require external resources when not available:
```python
@pytest.mark.skipif(
    not os.getenv("AWS_ACCESS_KEY_ID"),
    reason="Requires AWS credentials"
)
```

### Test Data

- Use small, synthetic datasets for tests
- Keep test data in `tests/fixtures/` or generate it in fixtures
- Clean up temporary files after tests

## Documentation

### Docstring Style

Use Google-style docstrings:

```python
def process_data(input_path: str, resolution: int = 10) -> str:
    """
    Process spatial data into H3-indexed format.
    
    Args:
        input_path: Path to input file
        resolution: H3 resolution level (0-15)
        
    Returns:
        Path to output file
        
    Raises:
        ValueError: If resolution is out of range
        
    Example:
        >>> output = process_data("data.parquet", resolution=10)
    """
```

### README Updates

When adding features, update:

1. `README.md` - Main project overview
2. `README_PACKAGE.md` - Detailed package documentation
3. Dataset-specific READMEs in subdirectories

## Pull Request Process

1. Update documentation for any new features

2. Add tests for new functionality

3. Ensure all tests pass

4. Update CHANGELOG.md (if exists)

5. Create pull request with:
   - Clear title describing the change
   - Description of what changed and why
   - Link to any related issues
   - Screenshots/examples if applicable

6. Address review feedback

## Code of Conduct

- Be respectful and inclusive
- Provide constructive feedback
- Focus on the code, not the person
- Help others learn and grow

## Questions?

- Open an issue for bugs or feature requests
- Start a discussion for questions or ideas
- Contact the maintainers: cboettig@berkeley.edu

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
