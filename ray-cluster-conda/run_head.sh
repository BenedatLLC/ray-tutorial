#!/bin/bash
set -e

if [ "`which ray`" = "" ]; then
  echo "Ray not found."
  echo "Perhaps you did not create and activate your conda environment."
  echo "You can do this via:"
  echo "conda env create -f envronment.yml"
  echo "conda activate ray-cluster-conda"
  exit 1
fi

if [ "$#" != "1" ]; then
  echo "$0 REDIS_PASSWORD"
  exit 1
fi

REDIS_PASSWORD=$1
ray start --head --redis-password="$REDIS_PASSWORD"
