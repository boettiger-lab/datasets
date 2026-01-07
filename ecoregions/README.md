```
cng-datasets workflow \
  --dataset ecoregions \
  --source-url /vsicurl/https://s3-west.nrp-nautilus.io/public-ecoregions/raw/ecoregions.gdb \
  --bucket public-ecoregions \
  --h3-resolution 8 \
  --hex-memory 32Gi \
  --max-completions 200 \
  --max-parallelism 100 \
  --parent-resolutions "9,8,0"
```