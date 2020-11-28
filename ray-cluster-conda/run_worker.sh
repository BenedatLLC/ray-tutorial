set -e

if [ "$#" != "2" ]; then
  echo "$0 HEAD_NODE_ADDRESS REDIS_PASSWORD"
  exit 1
fi

HEAD_NODE_ADDRESS=$1
REDIS_PASSWORD=$2

ray start --address=$HEAD_NODE_ADDRESS:6379 --redis-password="$REDIS_PASSWORD"
