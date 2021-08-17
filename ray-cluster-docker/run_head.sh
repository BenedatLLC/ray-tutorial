set -e

if [ "$#" != "2" ]; then
  echo "$0 REDIS_PASSWORD SHM_SIZE"
  exit 1
fi

REDIS_PASSWORD=$1
SHM=$2
echo "Using shared memory size of $SHM"

echo docker pull rayproject/ray:nightly
docker pull rayproject/ray:nightly

echo docker build -t ray-cluster-head -f Dockerfile.head .
docker build -t ray-cluster-head -f Dockerfile.head .


docker run --shm-size=$SHM -d  -v `cd ..;pwd`:/host \
  --network host \
  --name ray-head-container ray-cluster-head $REDIS_PASSWORD
docker ps
