# Contributing

Thank you for your interest in contributing to the CNG Datasets toolkit!

## Development Setup

1. **Clone the repository:**

```bash
git clone https://github.com/boettiger-lab/datasets.git
cd datasets
```

2. **Create a virtual environment:**

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Install in development mode:**

```bash
pip install -e ".[dev]"
```

4. **Install pre-commit hooks (optional):**

```bash
pip install pre-commit
pre-commit install
```

## Code Style

We use:
- **black** for code formatting (line length: 100)
- **ruff** for linting
- **mypy** for type checking

Format your code before committing:

```bash
black cng_datasets/
ruff check cng_datasets/
mypy cng_datasets/
```

## Testing

Run tests with pytest:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=cng_datasets

# Run specific test file
pytest tests/test_vector.py

# Run specific test
pytest tests/test_vector.py::test_h3_processor
```

### Writing Tests

- Place tests in the `tests/` directory
- Name test files `test_*.py`
- Use descriptive test names: `test_processor_handles_empty_input`
- Use fixtures for common setup
- Mock external dependencies (S3, Kubernetes)

Example:

```python
import pytest
from cng_datasets.vector import H3VectorProcessor

def test_processor_validates_resolution():
    with pytest.raises(ValueError):
        processor = H3VectorProcessor(
            input_url="test.parquet",
            output_url="output/",
            h3_resolution=20,  # Invalid resolution
        )
```

## Documentation

Documentation is built with Sphinx and hosted on GitHub Pages.

### Build Documentation Locally

```bash
cd docs/
pip install sphinx furo myst-parser
make html
```

View at `docs/_build/html/index.html`

### Documentation Guidelines

- Use Markdown for user guides
- Use reStructuredText for API docs
- Include code examples
- Add docstrings to all public functions/classes
- Follow Google docstring style

Example docstring:

```python
def process_chunk(self, chunk_id: int) -> str:
    """Process a specific chunk of the dataset.
    
    Args:
        chunk_id: Zero-based chunk index to process
        
    Returns:
        Path to the output parquet file
        
    Raises:
        ValueError: If chunk_id is out of range
        
    Example:
        >>> processor = H3VectorProcessor(...)
        >>> output = processor.process_chunk(0)
    """
```

## Pull Request Process

1. **Create a feature branch:**

```bash
git checkout -b feature/my-feature
```

2. **Make your changes:**
   - Write clean, documented code
   - Add tests for new functionality
   - Update documentation

3. **Run tests and linting:**

```bash
pytest
black cng_datasets/
ruff check cng_datasets/
```

4. **Commit your changes:**

```bash
git add .
git commit -m "Add feature: description"
```

5. **Push and create PR:**

```bash
git push origin feature/my-feature
```

Then create a Pull Request on GitHub.

### PR Checklist

- [ ] Code follows style guidelines
- [ ] Tests pass
- [ ] New tests added for new features
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] Commit messages are clear

## Reporting Issues

Use GitHub Issues to report bugs or request features.

### Bug Reports

Include:
- Description of the bug
- Steps to reproduce
- Expected behavior
- Actual behavior
- Python version and OS
- Relevant logs or error messages

### Feature Requests

Include:
- Clear description of the feature
- Use cases
- Example API (if applicable)

## Code of Conduct

- Be respectful and inclusive
- Welcome newcomers
- Focus on constructive feedback
- Assume good intentions

## Questions?

- Open a GitHub Issue for bugs/features
- Start a Discussion for questions
- Check existing issues before creating new ones

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
