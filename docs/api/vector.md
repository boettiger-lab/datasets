# Vector Processing API

Convert vector sources to H3-indexed GeoParquet directly from Python, with no Kubernetes involved. The `cng-datasets workflow` jobs call these same functions.

```{eval-rst}
.. automodule:: cng_datasets.vector
   :members:
   :show-inheritance:
   :special-members: __init__
```

## Converting to GeoParquet

`convert_to_parquet` is the convert step. It lives in its own submodule:
`from cng_datasets.vector.convert_to_parquet import convert_to_parquet`.

```{eval-rst}
.. autofunction:: cng_datasets.vector.convert_to_parquet.convert_to_parquet
```
