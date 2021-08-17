This is for running Ray under docker for a single node

To run Ray in a container, first run the script ``run.sh``. This will pull the ray docker image
and start the ray container. The ``ray-tutorial`` repository will be mouted inside the container
at ``/host``. You will be placed in a shell session inside the container. For example::

  $ ./run.sh
  Using default shared memory size of 1G
  docker pull rayproject/ray
  Using default tag: latest
  latest: Pulling from rayproject/ray
  Digest: sha256:c3b15b82825d978fd068a1619e486020c7211545c80666804b08a95ef7665371
  Status: Image is up to date for rayproject/ray:latest
  docker.io/rayproject/ray:latest
  docker run --shm-size=1G -it --rm -v /home/jfischer/code/ray-tutorial:/host rayproject/ray
  (base) ray@eab59651c661:~$


Once inside the container, we will start Ray and run the test_nodes.py script as follows::

   ray start --head --redis-password=test
   cd /host
   python test_nodes.py --num-actors=5 test

Note that we use ``test`` as the Redis password. Redis won't be visible outside of this container.
The output of these three commands should look like this::

  (base) ray@83bad82dce07:~$   ray start --head --redis-password=test
  Local node IP: 172.17.0.3
  2021-08-16 17:01:46,310    INFO services.py:1247 -- View the Ray dashboard at http://127.0.0.1:8265
  2021-08-16 17:01:46,312    WARNING services.py:1716 -- WARNING: The object store is using /tmp instead of /dev/shm because /dev/shm has only 1073741824 bytes available. This will harm performance! You may be able to free up space by deleting files in /dev/shm. If you are inside a Docker container, you can increase /dev/shm size by passing '--shm-size=10.24gb' to 'docker run' (or add it to the run_options list in a Ray cluster config). Make sure to set this to more than 30% of available RAM.
  
  --------------------
  Ray runtime started.
  --------------------
  
  Next steps
    To connect to this Ray runtime from another node, run
      ray start --address='172.17.0.3:6379' --redis-password='test'
  
    Alternatively, use the following Python code:
      import ray
      ray.init(address='auto', _redis_password='test')
  
    If connection fails, check your firewall settings and network configuration.
  
    To terminate the Ray runtime, run
      ray stop
  (base) ray@83bad82dce07:~$    cd /host
  (base) ray@83bad82dce07:/host$    python test_nodes.py --num-actors=5 test
  Initializing Ray...
  2021-08-16 17:01:50,730    INFO worker.py:801 -- Connecting to existing Ray cluster at address: 172.17.0.3:6379
  Nodes:
    83bad82dce07
  
  Getting placement group with 1 bundles...
  <ray.util.placement_group.PlacementGroup object at 0x7faee875b710>
  Running tasks...
  Found the following IPs: 172.17.0.3
  Waiting for 4 actors to finish
  Found the following IPs: 172.17.0.3
  Waiting for 3 actors to finish
  Found the following IPs: 172.17.0.3
  Waiting for 2 actors to finish
  Found the following IPs: 172.17.0.3
  Waiting for 1 actors to finish
  Found the following IPs: 172.17.0.3
  Completed successfully.

You will note that Ray was complaining about a small amount of shared memory. You can pass a larger shared memory size
as a command line argument to the ``run.sh`` script. For example::

  ./run.sh 10G

When you are done running Ray, you can just exist the shell. This will terminate the container (it was started with the `--rm`` option) and
put you back into the host's shell. For example::

  (base) ray@4d7e5fc641cc:~$ exit
  exit
  $

