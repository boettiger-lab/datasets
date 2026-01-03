#!/bin/bash


cng-datasets sync-job \
    --job-name sync2source \
    --source nrp:public-cpad \
    --destination source:us-west-2.opendata.source.coop/cboettig/cpad \
    --output sync-job.yaml

kubectl apply -f sync-job.yaml

kubectl logs -f job/sync2source



