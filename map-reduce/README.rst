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
my runs) and has lower maximum memory consumption. However, the code of the push design is more complex,
and took 579 lines of code, versus 428 lines for the pull version.

Downloading a Wikipedia dump
============================
The contents of the English Wikipedia is available as a single large (>76GB) XML file.
We can download and uncompress this file as follows::

  wget -O ./enwiki-latest-pages-articles.xml.bz2 http://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2
  bzip2 -d enwiki-latest-pages-articles.xml.bz2

Running the Examples
====================
Sequential Implementation
-------------------------
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

Ray Pull Implementation
-----------------------
The parallel implementation using the pull architecture requires the input Wikipedia dump file and
output file as positional arguments. The dump file is required to be in the same path on all machines
in the Ray cluster. This can be accomplished by copying the file to all nodes or by using a shared
filesystem (NFS). There are also a number of options related to configuring the
usage of Ray. Here is the full usage::


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
~~~~~~~
Start a new instance of Ray on this node and run with two workers for each pipeline stage::

   python3 ray_pull_map_reduce.py --num-mappers=2 --num-reducers=2 --num-sorters=2 enwiki-latest-page-articles.xml reference_counts.txt

Ray Push Implementation
-----------------------
Here is the full usage::

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

+----------+-----------+--------+------ ----+
| Node     | CPU Cores | Memory | Storage   |
+==========+===========+========+===========+
| Driver   |        8  |  32 GB | Hard disk |
+----------+-----------+--------+------ ----+
| Worker 1 |       16  |  64 GB | NVMe SSD  |
+----------+-----------+--------+------ ----+
| Worker 2 |       16  |  64 GB | NVMe SSD  |
+----------+-----------+--------+------ ----+

Here are the results:

+----------------+---------+----------+---------+-------------+
| Implementation | Mappers | Reducers | Sorters | Runtime (s) |
+================+=========+==========+=========+=============+
|    Sequential  |      1  |        1 |       1 |       1,059 |
+----------------+---------+----------+---------+-------------+
|    Parallel    |      1  |        1 |       1 |       1,996 |
+----------------+---------+----------+---------+-------------+
|    Parallel    |     10  |       10 |      10 |         302 |
+----------------+---------+----------+---------+-------------+
|    Parallel    |     20  |       10 |      10 |         740 |
+----------------+---------+----------+---------+-------------+
|    Parallel    |     20  |       20 |      20 |       1,022 |
+----------------+---------+----------+---------+-------------+




Implementation
==============
Our two examples do not parse the full XML syntax of the files. Instead they search for artricle
headings (inside of ``<title>`` tags) and references to other articles (inside of double braces,
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
The sequential implementation is straighforward: the mapper reads one article and the reducer
updates the counts for the references from that article. The sort is done at the end.

Parallel Implementation
-----------------------
The parallel implementation uses a Ray actor class for each of the
three stages of the map-reduce-sort pipeline. Each stage may have multiple workers,
allowing the program to take advantage of all the cores available
across a Ray cluster.

We divide the input file into "blocks", so that we can parallelize
the reading and processing of the file. Since a given reader might
start at an arbitrary point in the file, we read until we hit the first
``<title>`` tag. Then, we read lines until we reach the the end of the block.
This will likely include mulitple articles in the dataset. At the end of the
block, we keep reading until we hit the first ``<title>`` of the next block.
This ensures that we get the full content of the last article in our block.

Rather than pass the references from each individual article to the
reduce stage, the mappers build up article counters for a specific number of articles
scanned and then pass a "batch" to the reducers for those articles. The reducer for a given
article is determined by hashing the name of the article to an integer between 0 and
one less than ``num_reducers``.

When all the mappers have completed, the mapper actors are shut down and the coordinator
starts the sort actors. The coordinator asks one of the reducer actors for the distribution
of its counts, specifically the quantile boundries if we broke its data into equal-sized
groups, one for each of the sorters. The coordinator then asks each reducer to send its
data to the reducers, based on this grouping. Finally, the coordinator asks the sorters
for their sorted batches and writes them to the output file in sorted order.

