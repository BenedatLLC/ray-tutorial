#!/bin/bash
# Script to start ray worker inside container

if [ "$#" != "2" ]; then
  echo "$0: Error - two arguments required"
  echo "USAGE: $0 HEAD_NODE_ADDRESS REDIS_PASSWORD"
  exit 1
fi
HEAD_NODE_ADDRESS=$1
REDIS_PASSWORD=$2

echo "starting ray worker, to connect to head node $HEAD_NODE_ADDRESS"
ray start --address $HEAD_NODE_ADDRESS:6379  --block --verbose \
  --node-manager-port 6381 --object-manager-port 6382 \
  --min-worker-port 10200 --max-worker-port 10700 \
  --redis-password=$REDIS_PASSWORD
