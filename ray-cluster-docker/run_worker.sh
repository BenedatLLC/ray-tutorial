set -e

if [ "$#" != "3" ]; then
  echo "$0 HEAD_NODE_ADDRESS REDIS_PASSWORD SHM_SIZE"
  exit 1
fi

HEAD_NODE_ADDRESS=$1
REDIS_PASSWORD=$2
SHM=$3
echo "Using shared memory size of $SHM"

echo docker pull rayproject/ray
docker pull rayproject/ray

echo docker build -t ray-cluster-worker -f Dockerfile.worker .
docker build -t ray-cluster-worker -f Dockerfile.worker .

RUN_MODE_ARGS="-d --name ray-worker-container"
#RUN_MODE_ARGS="-it --rm"

docker run --shm-size=$SHM -d  -v `cd ..;pwd`:/host $RUN_MODE_ARGS \
  -p 6381:6381 -p 6382:6382 \
  -p 10200-10700:10200-10700 \
  ray-cluster-worker $HEAD_NODE_ADDRESS $REDIS_PASSWORD
docker ps
