#!/bin/bash

# exit if ray is already running
systemctl is-active --quiet ray
rc=$?
if [ "$rc" = "0" ]; then
  echo "Ray seems to already be running as a service!"
  exit 1
fi

THIS_DIR=`cd "$(dirname "$0")" && pwd`

if [ `uname` != "Linux" ]; then
  echo "This script only works on Linux"
  exit 1
fi

# double-check that we have a conda environment
if [ "`which ray`" = "" ]; then
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

function print_args_and_exit {
  echo "Command line invocation:"
  echo "  $0 head REDIS_PASSWORD"
  echo "  $0 worker REDIS_PASSWORD MASTER_NODE"
  exit 1
}
function check_for_script {
    if ! [ -f "$1" ]; then
        echo "Missing shell script at $1"
        exit 1
    fi
}

if [ "$#" -eq "0" ]; then
  echo "Script to setup a Ray service with systemd."
  print_args_and_exit
elif [ "$#" -eq "1" ]; then
  echo "Missing argument(s)"
  print_args_and_exit
fi
HEAD_OR_WORKER=$1

if [[ "$HEAD_OR_WORKER" == "head" ]]; then
  if [[ "$#" != "2" ]]; then
    echo "Wrong number of arguments for a head node"
    print_args_and_exit
  fi
  REDIS_PASSWORD=$2
  SCRIPT_TO_RUN="$THIS_DIR/run_head.sh"
  check_for_script "$SCRIPT_TO_RUN"
  echo "Installing systemd service for head"
elif [[ "$HEAD_OR_WORKER" == "worker" ]]; then
  if [[ "$#" != "3" ]]; then
    echo "Wrong number of arguments for a worker node"
    print_args_and_exit
  fi
  REDIS_PASSWORD=$2
  MASTER_NODE=$3
  SCRIPT_TO_RUN="$THIS_DIR/run_worker.sh"
  check_for_script "$SCRIPT_TO_RUN"
  echo "Installing systemd service for worker"
else
  echo "Node type must be 'head' or 'worker'"
  print_args_and_exit
fi

set -e

chmod +x $SCRIPT_TO_RUN

echo "  Startup command will be:"
echo "    $SCRIPT_TO_RUN $REDIS_PASSWORD 0.0.0.0 --block"

echo "  Writing configuration to ./ray.service.tmp"
/bin/cat > ./ray.service.tmp << EOM
[Unit]
   Description=Ray
[Service]
   Type=simple
   User=$USER
   ExecStart=$SCRIPT_TO_RUN $REDIS_PASSWORD 0.0.0.0 --block
[Install]
WantedBy=multi-user.target
EOM
echo "  Moving configuration to /lib/systemd/system/ray.service"
sudo mv ray.service.tmp /lib/systemd/system/ray.service
cat /lib/systemd/system/ray.service

echo "  Enabling ray to start at boot"
sudo systemctl enable ray

echo "  Starting ray.."
sudo systemctl start ray

echo "  Will sleep 5 seconds and then check status..."
echo sleep 5
sleep 5
echo systemctl status ray.service
systemctl status ray.service

echo "  Configuration completed successfully."
exit 0

