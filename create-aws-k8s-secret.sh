#!/bin/bash
# Script to create a Kubernetes secret for AWS credentials
# Usage: ./create-aws-k8s-secret.sh [secret-name] [namespace]

SECRET_NAME=${1:-aws-secret}
NAMESPACE=${2}

#read -p "Enter AWS Access Key ID: " AWS_ACCESS_KEY_ID
#read -s -p "Enter AWS Secret Access Key: " AWS_SECRET_ACCESS_KEY

echo

if [ -z "$NAMESPACE" ]; then
  kubectl create secret generic "$SECRET_NAME" \
    --from-literal=aws_access_key_id="$AWS_ACCESS_KEY_ID" \
    --from-literal=aws_secret_access_key="$AWS_SECRET_ACCESS_KEY"
else
  kubectl create secret generic "$SECRET_NAME" \
    --from-literal=aws_access_key_id="$AWS_ACCESS_KEY_ID" \
    --from-literal=aws_secret_access_key="$AWS_SECRET_ACCESS_KEY" \
    -n "$NAMESPACE"
fi

echo "Kubernetes secret '$SECRET_NAME' created successfully."
