

CPAD: California Protected Areas Database


```bash
wget https://data.cnra.ca.gov/dataset/0ae3cd9f-0612-4572-8862-9e9a1c41e659/resource/27323846-4000-42a2-85b3-93ae40edeff9/download/cpad_release_2025b.zip
unzip https://data.cnra.ca.gov/dataset/0ae3cd9f-0612-4572-8862-9e9a1c41e659/resource/27323846-4000-42a2-85b3-93ae40edeff9/download/cpad_release_2025b.zip
rclone copy CPAD_Release_2025b/ nrp:public-cpad/2025b/ -P 
```

```bash
cng-datasets workflow \
  --dataset cpad_2025b_holdings \
  --source-url https://s3-west.nrp-nautilus.io/public-cpad/2025b/CPAD_2025b_Holdings/CPAD_2025b_Holdings.shp \
  --bucket public-cpad \
  --h3-resolution 10 \
  --parent-resolutions "9,8,0" \
  --namespace gpn \
  --output-dir .
```

Then run the jobs:
```bash
# Optional: Create RBAC once (only needed first time)
kubectl apply -f workflow-rbac.yaml

# Run jobs in sequence:
kubectl apply -f convert-job.yaml
kubectl wait --for=condition=complete --timeout=3600s job/cpad-2025b-holdings-convert -n gpn

kubectl apply -f hex-job.yaml  
kubectl wait --for=condition=complete --timeout=7200s job/cpad-2025b-holdings-hex -n gpn

kubectl apply -f repartition-job.yaml
kubectl wait --for=condition=complete --timeout=3600s job/cpad-2025b-holdings-repartition -n gpn
```


CCED: California Conservation Easements Database

https://data.cnra.ca.gov/dataset/31b65732-941d-4af0-9d8c-279fac441fd6/resource/f906894e-bc00-4152-aba9-b33ac2934e4e/download/cced_2025b_release.zip