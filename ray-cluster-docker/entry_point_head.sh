#!/bin/bash
# Script to start ray head inside container
if [ "$#" = "1" ]; then
  REDIS_PW_ARG="--redis-password $1"
else
  REDIS_PW_ARG=""
fi

# need --dashboard-host inside of docker, see issue #7084
echo "starting ray"
ray start --head  --block --verbose --dashboard-host 0.0.0.0 \
  --gcs-server-port 6380 --node-manager-port 6381 --object-manager-port 6382 \
  --min-worker-port 10200 --max-worker-port 10700 $REDIS_PW_ARG
