set -e

if [ "$#" = "1" ]; then
  SHM=$1
  echo "Using shared memory size of $SHM"
else
  SHM='1G'
  echo "Using default shared memory size of $SHM"
fi

RAY_IMAGE=ray-ml # image containing most of the ML libraries you will need

echo docker pull rayproject/$RAY_IMAGE
docker pull rayproject/$RAY_IMAGE

echo docker run --shm-size=$SHM -it --rm -v `cd ..;pwd`:/host rayproject/$RAY_IMAGE
docker run --shm-size=$SHM -it --rm -v `cd ..;pwd`:/host rayproject/$RAY_IMAGE
