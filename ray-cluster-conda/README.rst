Ray Cluster Using Anaconda
==========================
In this example, we create a Ray cluster using the
`Anaconda distribution <https://www.anaconda.com/products/individual>`_.
to set up the software on each machine.

Anaconda Setup
--------------
First, install the Anaconda distribution on each machine of your cluster.
Once you have installed Anaconda on each machine, create the Conda environment::

  conda env create -f environment.yml

Once the environment has been created, activate it via::

  conda activate ray-cluster-conda

If you get an error like::

  RuntimeError: Version mismatch: The cluster was started with:
      Ray: 1.5.1.post1
      Python: 3.8.5
  This process on node 192.168.1.38 was started with:
      Ray: 1.5.0
      Python: 3.8.5

Check and see if you have a version mismatch. You can try upgrading the node with the older
version or destroy the conda environment on that node, edit the enviroment.yml file to
pin the versions of Ray and Python, and then recreate the environment.

Installing a systemd service
----------------------------
After creating the conda environment on each node, you can set up a systemd service to permit automatic
startup of Ray during boot. The script ``setup_service.sh`` will install Ray as a systemd service (called "ray") on Linux.
To install on the head node, run::

  ./setup_service.sh head REDIS_PASSWORD

where ``REDIS_PASSWORD`` is your Redis password for the Ray head.

To install on a worker node, run::

  echo "  $0 worker REDIS_PASSWORD MASTER_NODE

where ``MASTER_NODE`` is the hostname or IP address of the master node.

Destroying the Conda environment
--------------------------------
To destroy the conda environment, run::

  conda remove --name ray-cluster-conda --all

