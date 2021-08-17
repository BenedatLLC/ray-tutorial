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

if [ "$#" = "1" ]; then
  REDIS_PASSWORD=$1
  DASHBOARD_OPT=""
elif [ "$#" = "2" ]; then
  REDIS_PASSWORD=$1
  DASHBOARD_OPT="--dashboard-host=$2"
  BLOCK_OPT=""
elif [ "$#" = "3" ]; then
  REDIS_PASSWORD=$1
  DASHBOARD_OPT="--dashboard-host=$2"
  if [ "$3" = "--block" ]; then
    BLOCK_OPT="--block"
  else
    echo "$0 REDIS_PASSWORD [DASHBOARD_HOST] [--block]"
    exit 1
  fi
else
  echo "$0 REDIS_PASSWORD [DASHBOARD_HOST] [--block]"
  exit 1
fi

REDIS_PASSWORD=$1
ray start --head --redis-password="$REDIS_PASSWORD" $DASHBOARD_OPT $BLOCK_OPT
