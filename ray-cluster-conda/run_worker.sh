set -e
if [ "`which ray`" = "" ]; then
  echo "Ray not found."
  echo "Perhaps you did not create and activate your conda environment."
  echo "You can do this via:"
  echo "conda env create -f envronment.yml"
  echo "conda activate ray-cluster-conda"
  exit 1
fi

if [ "$#" != "2" ]; then
  echo "$0 HEAD_NODE_ADDRESS REDIS_PASSWORD"
  exit 1
fi

HEAD_NODE_ADDRESS=$1
REDIS_PASSWORD=$2

ray start --address=$HEAD_NODE_ADDRESS:6379 --redis-password="$REDIS_PASSWORD"
