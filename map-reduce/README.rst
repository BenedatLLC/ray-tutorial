==================
Map Reduce Example
==================

Here, we have three implementaitons of a map-reduce-sort for building a
list of all the articles in Wikipedia with their incoming reference counts,
sorted by the most refences and then by article title. The three implementations are:

1. ``sequential_map_reduce.py`` is purely sequential and does not use Ray
2. ``ray_pull_map_reduce.py`` is a parallel Ray implementation where a single sorter
   calls multiple reducers, requesting data. Each reducer calls multiple mappers,
   requesting data. Thus, the data is "pulled" along the pipelne.
3. ``ray_push_map_reduce.py`` is a parallel Ray implementation with multiple mappers,
   reducers, and sorters. The mappers start by reading the input file and make repeated
   calls to the reducers to "push" their data. The reducers then send their results to the
   sorters.

``ray_push_map_reduce.py`` is a little faster than ``ray_pull_map_reduce.py`` (about 6% in
my runs) and has lower maximum memory consumption. If the data set did not fit into a
node's memory, this would be the preferred implementation.
However, the code of the push design is more complex,
and took 579 lines of code, versus 428 lines for the pull version.

Implementation
==============
Our examples do not parse the full XML syntax of the files. Instead they search for article
headings (inside of ``<title>`` tags) and references to other articles (inside of double brackets,
like ``[[this]]``). The "map" part of map-reduce reads from the dump file and yields a stream of
articles and the references they contain. We can drop the article and consider only the references,
which each represent one incoming reference for each article targeted.

The "reduce" part of the algorithm maintains a ``Counter`` of articles and their reference counts.
For each result yielded from the mapper is used to increment the counts of the referenced articles.
When the entire dump file has been mapped and reduced, we can sort the (article, count) pairs by
count decreasing (highest count first) and article increasing (alphabetical order within a count
value) and write the result to a csv file.

Sequential Implementation
-------------------------
The sequential implementation is straighforward: the mapper reads one article at a time
and the reducer updates the counts for the references from that article. The sort is done at the end.

Parallel Implementations
------------------------
The parallel implementations use Ray actor classs for for the map and reduce stages.
Each stage may have multiple copies of the map and reduce actors, allowing the program to take
advantage of all the cores available across a Ray cluster.
We divide the input file into "blocks", so that we can parallelize
the reading and processing of the file. Since a given reader might
start at an arbitrary point in the file, we read until we hit the first
``<title>`` tag. Then, we read lines until we reach the the end of the block.
This will likely include mulitple articles in the dataset. At the end of the
block, we keep reading until we hit the first ``<title>`` of the next block.
This ensures that we get the full content of the last article in our block.

Each mapper maintains a ``Counter`` for each reducer. The reducer for a given
article is determined by a simple hashing algorithm on the article name.

Both parallel implementations use `placement groups <https://docs.ray.io/en/latest/placement-group.html>`_
to spread the actors across the nodes of the Ray cluster. To do this, we find the smallest node
in the cluster (in terms of CPU count) and create a placement group with that CPU count per node.
When starting the individual actors, we use the ``placement_group_bundle_index`` option to explictly
perform a round-robin mapping. If you don't want to use placement groups, you can use the
``--skip-placement-groups`` option to disable this behavior.

Pull Implementation
~~~~~~~~~~~~~~~~~~~
The pull implementation runs a mapper per CPU in the placement group, a reducer per CPU in the placement
group, and a single sorter on the driver node. the sorter makes a single request for each reducer.
Each reducer, in turn, makes a single request per mapper. When the first request is made to a given
mapper, it processes its entire block, caching the Counters for all reducer in memory. It then
returns the counter for the first requesting reducer. Subsequent calls just return Counters from
its cache.

Push Implementation
~~~~~~~~~~~~~~~~~~~
In the push implementation, mappers drive the workflow. Each mapper reads its articles
and builds up a ``Counter`` per reducer. When a specified number of articles have been processed
(2000 by default), the mapper sends its counters to the reducers and then clears its local copy.
The reducers process each batch as it arrives until all have been receivoed from the mappers.

When all the mappers have completed, the mapper actors are dereferenced and the coordinator
starts the sort actors. The coordinator asks one of the reducer actors for the distribution
of its counts, specifically the quantile boundries if we broke its data into equal-sized
groups, one for each of the sorters. The coordinator then asks each reducer to send its
data to the reducers, based on this grouping. Finally, the coordinator asks the sorters
for their sorted batches and writes them to the output file in sorted order.

Running the Examples
====================
Downloading a Wikipedia dump
----------------------------
The contents of the English Wikipedia is available as a single large (>76GB) XML file.
We can download and uncompress this file as follows::

  wget -O ./enwiki-latest-pages-articles.xml.bz2 http://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2
  bzip2 -d enwiki-latest-pages-articles.xml.bz2

Sequential Version
------------------
The sequential implementation can be run from the command line. It takes two positional arguments:
name of the the dump (input) file and the name of the output file. Here is the full usage::

  python3 sequential_map_reduce.py [-h] [--verbose] DUMP_FILE OUTPUT_FILE
  
  positional arguments:
    DUMP_FILE    Name of file containing wikipedia dump
    OUTPUT_FILE  Name to use for output csv file
  
  optional arguments:
    -h, --help   show help message and exit
    --verbose    Is specified, print extra debugging

Example
~~~~~~~
Here is an example that puts the output in the file reference_counts.csv::

  python3 seuential_map_reduce.py enwiki-latest-pages-articles.xml reference_counts.csv

Ray-based Parallel Versions
---------------------------
The parallel implementations take two positional arguments: the Wikipedia dump file to be read
and the output file to write the results. The dump file is required to be in the same path on all machines
in the Ray cluster. This can be accomplished by copying the file to all nodes or by using a shared
filesystem (NFS). The output file will be written on the driver node.

There are also a few options related to configuring the usage of Ray. In particular,
``--address`` can be used to specify the ``address`` parameter to ``ray.init()``. If you have a password
configured for Redis, you can use the option ``--redis-password`` to specify this password.


Pull Version
~~~~~~~~~~~~
The pull implelmentation will start one mapper and one reducer per CPU in the placement group.
The sorter is run on the driver node. Here are the full command line arguments::


  ray_pull_map_reduce.py [-h] [--redis-password REDIS_PASSWORD] [--address ADDRESS] [--skip-placement-groups]
                                [--pct-pending-requests PCT_PENDING_REQUESTS] [--verbose]
                                DUMP_FILE OUTPUT_FILE
  
  positional arguments:
    DUMP_FILE             Name of file containing wikipedia dump. This must be in the same location across all the nodes of the Ray
                          cluster.
    OUTPUT_FILE           Name to use for output csv file
  
  optional arguments:
    -h, --help            show this help message and exit
    --redis-password REDIS_PASSWORD
                          Password to use for Redis, if non-default
    --address ADDRESS     Address for this Ray node, defaults to 'auto'
    --skip-placement-groups
                          If specified, don't use placement groups
    --pct-pending-requests PCT_PENDING_REQUESTS
                          Fraction of pending requests to wait for, as a percentage of outstanding requests. If not specified, will
                          wait for 50 percent of the outstanding requests
    --verbose             Is specified, print extra debugging

Example
.......
Run the pull implementation, where the Redis password is "foo23", the Wikipedia dump is stored in the ``/data`` on all nodes,
and the output is written to the current directory on the driver::

   python3 ray_pull_map_reduce.py --redis-password=foo23 enwiki-latest-page-articles.xml reference_counts.csv


Ray Push Version
~~~~~~~~~~~~~~~~
Here is the full usage for the push implementation::

  ray_push_map_reduce.py [-h] [--redis-password REDIS_PASSWORD] [--address ADDRESS]
                                [--articles-per-mapper-batch ARTICLES_PER_MAPPER_BATCH] [--flow-control] [--skip-placement-groups]
                                [--verbose]
                                DUMP_FILE OUTPUT_FILE
  
  positional arguments:
    DUMP_FILE             Name of file containing wikipedia dump. This must be in the same location across all the nodes of the Ray
                          cluster.
    OUTPUT_FILE           Name to use for output csv file
  
  optional arguments:
    -h, --help            show this help message and exit
    --redis-password REDIS_PASSWORD
                          Password to use for Redis, if non-default
    --address ADDRESS     Address for this Ray node, defaults to 'auto'
    --articles-per-mapper-batch ARTICLES_PER_MAPPER_BATCH
                          Number of articles to read from dump file in each mapper batch, defaults to 2000
    --flow-control        If specified, mappers will wait for reducers to acknowlege batches before continuing.
    --skip-placement-groups
                          If specified, don't use placement groups
    --verbose             Is specified, print extra debugging

    
Performance Tests
=================
To evaluate the map-reduce implementations, I ran them on a 3 node cluster:

+----------+-----------+--------+-----------+
| Node     | CPU Cores | Memory | Storage   |
+==========+===========+========+===========+
| Head     |        8  |  32 GB | Hard disk |
+----------+-----------+--------+-----------+
| Worker 1 |       16  |  64 GB | NVMe SSD  |
+----------+-----------+--------+-----------+
| Worker 2 |       16  |  64 GB | NVMe SSD  |
+----------+-----------+--------+-----------+

I ran the sequential version on one of the worker nodes.

The head node is a much older machine. To keep the results more balanced (and to fully
utilize the bigger nodes with placement groups), I set the
``--num-cpus`` option to 0 on the head node, so all the workers run only on the two
worker nodes. Thus, the Ray versions allocated 32 mappers and 32 reducers total.

I ran each scenario three times and took the mean and standard deviation. Here are the results:

+----------------------------+-------------+------------+
| Implementation             | Runtime (s) | StdDev (s) |
+============================+=============+============+
| Sequential                 |      1065.7 |        4.9 |
+----------------------------+-------------+------------+
| Pull with placement groups |       179.7 |        0.7 |
+----------------------------+-------------+------------+
| Pull w/o placement groups  |       208.7 |       26.8 |
+----------------------------+-------------+------------+
| Push with placement groups |       168.7 |        0.6 |
+----------------------------+-------------+------------+


