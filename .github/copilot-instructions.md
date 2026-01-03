## Local environment

Remember to activate the `.venv` in the repository root for python commands.

When using GDAL for remote files, remember to use VSI-style operations for efficient streaming.  Use `/vsicurl/` on public URLs over `/vsis3/`

## S3

- You are running on a local laptop with access to a kubernetes cluster, Nautilus, which also has it's own s3 bucket system.  

- Most buckets are already set for anonymous public access. The public S3 endpoint for the NRP cluster is https://s3-west.nrp-nautilus.io.  Be sure to configure "path" style instead of the default "vhost" style URLs for the software.  

- Authentication environmental variables are not set by default on the local machine.  For read-only operations anonymous access should be sufficient; be sure to set the endpoint correctly first. 

- Occassionally we will use other S3 endpoints, which will be explicitly referenced.

- Note that jobs that run on k8s, inside the cluster, use an internal s3 endpoint, `rook-ceph-rgw-nautiluss3.rook`.  This is the same s3 storage system, but provides much faster access but only to pods on the k8s cluster.  It uses plain http (no SSL), and path (not vhost) addressing.  The S3 authentication keys are already available as secrets on the k8s cluster under the `aws` secret, load these secrets when necessary for write access or bucket operations. 


## k8s

- `kubectl` is already configured with the default namespace (biodiversity).  It's the only ns we have permission for and does not need to be set explicitly.  