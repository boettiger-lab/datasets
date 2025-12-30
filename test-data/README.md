cng-datasets workflow \
  --dataset test-data \
  --source-url https://dsl.richmond.edu/panorama/redlining/static/mappinginequality.gpkg \
  --bucket public-test \
  --h3-resolution 10 \
  --parent-resolutions "9,8,0" \
  --hex-memory 8Gi \
  --max-completions 200 \
  --parent-resolutions "9,8,0"
```