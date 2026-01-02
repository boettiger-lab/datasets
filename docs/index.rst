CNG Datasets Toolkit
=====================

A Python toolkit for processing large geospatial datasets into cloud-native formats with H3 hexagonal indexing.

.. toctree::
   :maxdepth: 2
   :caption: User Guide

   installation
   quickstart
   vector_processing
   raster_processing
   kubernetes_workflows
   configuration

.. toctree::
   :maxdepth: 2
   :caption: API Reference

   api/vector
   api/raster
   api/k8s
   api/storage

.. toctree::
   :maxdepth: 1
   :caption: Additional Resources

   examples
   contributing
   changelog
   api_reference

Features
--------

* **Vector Processing**: Convert polygon and point datasets to H3-indexed GeoParquet
* **Raster Processing**: Create Cloud-Optimized GeoTIFFs (COGs) and H3-indexed parquet
* **Kubernetes Integration**: Generate and submit K8s jobs for large-scale processing
* **Cloud Storage**: Manage S3 buckets and sync across multiple providers with rclone
* **Scalable**: Chunk-based processing for datasets that don't fit in memory

Quick Links
-----------

* :doc:`installation`
* :doc:`quickstart`
* :doc:`api_reference`
* `GitHub Repository <https://github.com/boettiger-lab/datasets>`_
* `Browse Data Catalog (STAC) <stac/index.html>`_

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
