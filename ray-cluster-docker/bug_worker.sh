set -e
REDIS_PASSWORD=
THIS_NODE_IP=
HEAD_NODE_IP=

echo docker pull rayproject/ray
docker pull rayproject/ray


docker run --shm-size=2G -v `cd ..;pwd`:/host -it --rm \
  -p 6381:6381 -p 6382:6382 -p 10200-10700:10200-10700 \
  rayproject/ray:latest ray start --address $HEAD_NODE_IP:6379 \
   --node-ip-address $THIS_NODE_IP --block --verbose \
   --node-manager-port 6381 --object-manager-port 6382 \
   --min-worker-port 10200 --max-worker-port 10700 \
   --redis-password=$REDIS_PASSWORD
