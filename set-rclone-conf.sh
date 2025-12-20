#!/bin/bash
# Expand environment variables in rclone.conf and create secret
kubectl create secret generic rclone-config --from-file=rclone.conf=/dev/stdin --dry-run=client -o yaml | kubectl apply -f -
