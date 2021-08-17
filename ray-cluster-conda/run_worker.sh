#!/bin/bash

# check for our conda executable and set path if needed
if [ "`/usr/bin/which conda`" = "" ]; then
    if [ -x "$HOME/anaconda3/bin/conda" ]; then
        export PATH="$PATH:$HOME/anaconda3/bin"
    else
        echo "Cound not find conda executable in path or at $HOME/anaconda3/bin/conda"
        exit 1
    fi
fi

# check for ray and set anaconda environment if needed
if [ "`/usr/bin/which ray`" = "" ]; then
    echo "Ray not found, attempting to activate ray-cluster-conda"
    eval "$(conda shell.bash hook)"
    conda activate ray-cluster-conda
    rc=$?
    if [[ "$rc" == "1" ]]; then
        echo "Unable to activate conda environment ray-cluster-conda"
        echo "Perhaps you did not create conda environment."
        echo "You can do this via:"
        echo "conda env create -f envronment.yml"
        exit 1
    fi
fi
set -e

if [ "$#" = "2" ]; then
  HEAD_NODE_ADDRESS=$1
  REDIS_PASSWORD=$2
  BLOCK_OPT=""
elif [ "$#" = "3" ]; then
    HEAD_NODE_ADDRESS=$1
    REDIS_PASSWORD=$2
    if [ "$3" = "--block" ]; then
        BLOCK_OPT="--block"
    else
        echo "$0 HEAD_NODE_ADDRESS REDIS_PASSWORD [--block]"
        exit 1
    fi
else
  echo "$0 HEAD_NODE_ADDRESS REDIS_PASSWORD [--block]"
  exit 1
fi


ray start --address=$HEAD_NODE_ADDRESS:6379 --redis-password="$REDIS_PASSWORD" $BLOCK_OPT
