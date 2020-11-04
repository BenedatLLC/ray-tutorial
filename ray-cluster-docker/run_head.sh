set -e

if [ "$#" != "2" ]; then
  echo "$0 REDIS_PASSWORD SHM_SIZE"
  exit 1
fi

REDIS_PASSWORD=$1
SHM=$2
echo "Using shared memory size of $SHM"

echo docker pull rayproject/ray
docker pull rayproject/ray

echo docker build -t ray-cluster-head -f Dockerfile.head .
docker build -t ray-cluster-head -f Dockerfile.head .


#docker run --shm-size=$SHM -it --rm -v `cd ..;pwd`:/host \
#  -p 8265:8265 -p 6379:6379 -p 6380:6380 -p 6381:6381 -p 6382:6382 \
#  ray-cluster-head /host/ray-cluster-docker/entry_point_head.sh
docker run --shm-size=$SHM -d  -v `cd ..;pwd`:/host \
  -p 8265:8265 -p 6379:6379 -p 6380:6380 -p 6381:6381 -p 6382:6382 \
  -p 10200-10700:10200-10700 \
  --name ray-head-container ray-cluster-head $REDIS_PASSWORD
docker ps
