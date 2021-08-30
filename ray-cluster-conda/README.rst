Ray Cluster Using Anaconda
==========================

In this example, we create a Ray cluster using the
`Anaconda distribution <https://www.anaconda.com/products/individual>`_.
to set up the software on each machine.

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

Check and see if you have a version missmatch. You can try upgrading the node with the older
version or destroy the conda environment on that node, edit the enviroment.yml file to
pin the versions of Ray and Python, and then recreate the environment.

To destroy the conda environment, run::

  conda remove --name ray-cluster-conda --all


