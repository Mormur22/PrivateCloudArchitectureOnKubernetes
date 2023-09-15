#!/bin/bash

# Extract PVC names using awk and pass them to kubectl delete
kubectl get pvc | awk '/datadir-my-release-mongodb-sharded/ {print $1}' | xargs kubectl delete pvc
