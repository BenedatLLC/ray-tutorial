set -e

if [ "$#" != "3" ]; then
  echo "$0 HEAD_NODE_ADDRESS REDIS_PASSWORD SHM_SIZE"
  exit 1
fi

HEAD_NODE_ADDRESS=$1
REDIS_PASSWORD=$2
SHM=$3
echo "Using shared memory size of $SHM"

echo docker pull rayproject/ray:nightly
docker pull rayproject/ray:nightly

echo docker build -t ray-cluster-worker -f Dockerfile.worker .
docker build -t ray-cluster-worker -f Dockerfile.worker .

RUN_MODE_ARGS="-d --name ray-worker-container"
# Use this version for debugging
#RUN_MODE_ARGS="-it --rm"

docker run --shm-size=$SHM -v `cd ..;pwd`:/host $RUN_MODE_ARGS \
   --network host \
   ray-cluster-worker $HEAD_NODE_ADDRESS $REDIS_PASSWORD
docker ps
