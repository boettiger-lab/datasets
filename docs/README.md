# Documentation

This directory contains the Sphinx documentation for the CNG Datasets toolkit.

## Building Locally

### Install Dependencies

```bash
cd docs/
pip install -r requirements.txt
```

Or install the docs optional dependencies:

```bash
pip install -e ".[docs]"
```

### Build HTML Documentation

```bash
cd docs/
make html
```

The built documentation will be in `_build/html/`. Open `_build/html/index.html` in your browser to view.

### Live Rebuild (Development)

For automatic rebuilding while editing:

```bash
cd docs/
make livehtml
```

This will start a local server at http://127.0.0.1:8000 with live reloading.

## Documentation Structure

- `index.rst` - Main documentation index
- `installation.md` - Installation instructions
- `quickstart.md` - Quick start guide
- `vector_processing.md` - Vector processing guide
- `raster_processing.md` - Raster processing guide
- `kubernetes_workflows.md` - Kubernetes workflows guide
- `configuration.md` - Configuration guide
- `examples.md` - Real-world examples
- `api/` - API reference documentation
- `contributing.md` - Contributing guidelines
- `changelog.md` - Version history

## Configuration

Documentation is configured in `conf.py`:

- **Theme**: Furo (modern, responsive theme)
- **Extensions**: 
  - `sphinx.ext.autodoc` - Auto-generate API docs from docstrings
  - `sphinx.ext.napoleon` - Support Google/NumPy docstring styles
  - `sphinx.ext.viewcode` - Add links to source code
  - `sphinx.ext.intersphinx` - Link to other project docs
  - `myst_parser` - Support Markdown files

## Deployment

Documentation is automatically built and deployed to GitHub Pages on every push to the `main` branch via GitHub Actions (`.github/workflows/docs.yml`).

View the published documentation at: https://boettiger-lab.github.io/datasets/

## Writing Documentation

### Markdown Files

Most user-facing documentation uses Markdown (`.md` files). MyST parser provides additional features:

```markdown
# Section

Regular markdown content.

:::{note}
This is a note admonition.
:::

## Code Examples

\`\`\`python
from cng_datasets.vector import H3VectorProcessor
\`\`\`
```

### API Documentation

API documentation is auto-generated from Python docstrings. Use Google-style docstrings:

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

## Troubleshooting

### Build Errors

If you get module import errors, make sure the package is installed:

```bash
pip install -e ..
```

### Missing Dependencies

Install all documentation dependencies:

```bash
pip install -r requirements.txt
```

### Warnings About Missing Static Files

Create empty `_static/` directory if it doesn't exist:

```bash
mkdir -p _static
```
