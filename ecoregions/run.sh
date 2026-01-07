#!/bin/bash

source ../.venv/bin/activate

cng-datasets workflow \
  --dataset ecoregion \
  --source-url /vsicurl/https://s3-west.nrp-nautilus.io/public-ecoregions/raw/ecoregions.gdb \
  --bucket public-ecoregion \
  --h3-resolution 8 \
  --hex-memory 32Gi \
  --max-completions 200 \
  --max-parallelism 100 \
  --parent-resolutions "9,8,0"

