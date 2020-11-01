set -e

if [ "$#" = "1" ]; then
  SHM=$1
  echo "Using shared memory size of $SHM"
else
  SHM='1G'
  echo "Using default shared memory size of $SHM"
fi

echo docker pull rayproject/ray
docker pull rayproject/ray

echo docker build -t ray-tutorial .
docker build -t ray-tutorial .

echo docker run --shm-size=$SHM -it --rm -v `cd ..;pwd`:/host ray-tutorial
docker run --shm-size=$SHM -it --rm -v `cd ..;pwd`:/host ray-tutorial
