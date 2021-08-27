#!/usr/bin/env python3
"""This program implements a parallel map-reduce-sort for
building a sorted list of Wikipedia articles and their inbound
reference counts. It uses a Ray actor class for each of these
three stages of the pipeline. Each stage may have multiple workers,
allowing the program to take advantage of all the cores available
across a Ray cluster.
"""
import re
import sys
from enum import Enum
import argparse
import os
from os.path import abspath, expanduser, exists
from math import ceil
from collections import Counter
import time
from typing import TextIO, Generator, Tuple, Set, Optional

import ray
import numpy as np
import pandas as pd

DEFAULT_BLOCK_SIZE = 4096

# Regular expresssion to look for titles and references
PATTERN = r"(?:<title>.+?</title>)|(?:\[\[.+?\]\])"
RE = re.compile(PATTERN)


class ReadState(Enum):
    INITIAL = 0
    READING_BODY = 1
    READING_UNTIL_NEXT_TITLE = 2


def line_reader(f: TextIO, offset: int):
    """This wraps the reading of lines for a block in the file.
    It handles the first line differently -- we seek to the specified
    start offset and try to read the line. Since we are starting
    at, what is really a random byte in the file, we might be starting
    at the middle of a UTF-8 character. Thus, if we get a decode error,
    we advance again until we get to the end of the character and can read
    the line. The higher level protocol implemented by read_block() ensures
    that we won't ever miss an article.
    """
    for i in range(4):
        f.seek(offset + i)
        try:
            yield next(f)
            break
        except UnicodeDecodeError as e:
            if i == 3:
                raise
            else:
                print(f"Got decode error {e} at offset {offset}, will try next byte")
    # the rest of the lines are handled normally.
    for line in f:
        yield line


def read_block(
    f: TextIO, block_size: int, offset: int = 0, verbose: bool = False
) -> Generator[Tuple[str, Set[str]], None, None]:
    """We break the input file into "blocks", so that we can parallelize
    the reading and processing of the file. Since a given reader might
    start at an arbitrary point in the file, we read until we hit the first
    <title> tag. Then, we read lines until we reach the the end of the block.
    This will likely include mulitple articles in the dataset. At the end of the
    block, we keep reading until we hit the first <title> of the next block.
    This ensures that we get the full content of the last article in our
    block.
    """
    if verbose:
        print(f"read_block(offset={offset}, size={block_size})")
    if offset != 0:
        reader = line_reader(f, offset)
    else:
        reader = f
    state = ReadState.INITIAL
    current_page: Optional[str] = None
    references: Set[str] = set()
    chars_read = 0
    for line in reader:
        chars_read += len(line)
        matches = RE.findall(line)
        for match in matches:
            if match.startswith("<ti"):
                new_page = match[7:-8]
                if state == ReadState.INITIAL:
                    current_page = new_page
                    state = ReadState.READING_BODY
                elif state == ReadState.READING_BODY:
                    assert current_page is not None
                    yield (current_page, references)
                    current_page = new_page
                    references = set()
                else:
                    assert state == ReadState.READING_UNTIL_NEXT_TITLE
                    if verbose:
                        print(f"Hit next title {new_page}, yielding {current_page}")
                    assert current_page is not None
                    yield (current_page, references)
                    return  # we hit the first title of the next block
            else:
                assert match.startswith("[[")
                if state == ReadState.INITIAL:
                    continue  # we skip until we reach a title
                body = match[2:-2]
                if ":" in body:  # we skip the special tags
                    if verbose:
                        print(f"skipping {body}")
                    continue
                references.add(body.split("|")[0].strip())
        if chars_read >= block_size:
            if state == ReadState.INITIAL:
                if verbose:
                    print("did not find anything in block")
                return  # did not find anything in this block
            elif state == ReadState.READING_BODY:
                state = ReadState.READING_UNTIL_NEXT_TITLE
                if verbose:
                    print(f"hit end of page, but still reading article {current_page}")
    # reached the end of the file
    if current_page:
        yield (current_page, references)


def get_reducer(article: str, num_reducers: int) -> int:
    """We map an article title to a reducer. This is done via hashing."""
    return (sum([ord(s) for s in article]) + len(article)) % num_reducers


@ray.remote
class Mapper:
    """Each mapper is an actor that reads from a block of the input file, builds a
    counter per reducer of article reference counts, and then periodically pushes
    these to the reducers.
    """

    def __init__(
            self, mapper_id: int, dump_file: str, reducers, articles_per_batch: int, verbose: bool = False
    ):
        self.mapper_id = mapper_id
        self.dump_file = dump_file
        assert exists(
            dump_file
        ), f"Mapper did not find dump file {dump_file}. Is it on the same path for all nodes?"
        self.reducers = reducers  # an array of reducers
        self.articles_per_batch = articles_per_batch
        self.verbose = verbose

    def get_host(self):
        import socket
        return socket.gethostname()

    def map(self, block_size, offset):
        reduce_futures = []
        num_reducers = len(self.reducers)
        counters = [Counter() for c in range(num_reducers)]

        def send_batch(futures):
            if len(futures) > 0:
                # we make sure the previous batch of reduce calls have completed
                ray.get(futures)
            new_futures = []
            for (reducer, counter) in enumerate(counters):
                if len(counter) > 0:
                    new_futures.append(self.reducers[reducer].reduce.remote(counter))
                    if self.verbose:
                        print(
                            f"Mapper[{offset}] send {len(counter)} to reducer {reducer}"
                        )
            return new_futures

        with open(self.dump_file, "r") as f:
            articles_in_batch = 0
            for (article, references) in read_block(
                f, block_size, offset, verbose=self.verbose
            ):
                for ref_article in references:
                    counters[get_reducer(ref_article, num_reducers)][
                        ref_article
                    ] += 1
                articles_in_batch += 1
                if articles_in_batch == self.articles_per_batch:
                    reduce_futures = send_batch(reduce_futures)
                    articles_in_batch = 0
                    counters = [Counter() for c in range(num_reducers)]

            if articles_in_batch > 0:
                reduce_futures = send_batch(reduce_futures)
            if len(reduce_futures) > 0:
                ray.get(reduce_futures)
            print(f"Mapper[{self.mapper_id}] completed")

    def map_no_flow_control(self, block_size, offset):
        num_reducers = len(self.reducers)
        counters = [Counter() for c in range(num_reducers)]

        def send_batch():
            for (reducer, counter) in enumerate(counters):
                if len(counter) > 0:
                    self.reducers[reducer].reduce_no_flow_control.remote(self.mapper_id, counter)
                    if self.verbose:
                        print(
                            f"Mapper[{offset}] send {len(counter)} to reducer {reducer}"
                        )

        with open(self.dump_file, "r") as f:
            articles_in_batch = 0
            for (article, references) in read_block(
                f, block_size, offset, verbose=self.verbose
            ):
                for ref_article in references:
                    counters[get_reducer(ref_article, num_reducers)][
                        ref_article
                    ] += 1
                articles_in_batch += 1
                if articles_in_batch == self.articles_per_batch:
                    send_batch()
                    articles_in_batch = 0
                    counters = [Counter() for c in range(num_reducers)]

            if articles_in_batch > 0:
                send_batch()
        final_futures = [reducer.end_of_reduce_calls.remote(self.mapper_id) for reducer in self.reducers]
        ray.get(final_futures)
        print(f"Mapper[{self.mapper_id}] completed")

REDUCE_PRINT_FREQUENCY=10000

@ray.remote
class Reducer:
    """Reducers receive batches of article counts from the mappers.
    They just combine these to build up a single count. Due to the article
    hashing, each reducer will have all the counts for a subset of the articles.
    After the mapping is done, the coordinator will ask for the distribution from
    one reducer. In the final phase, each reducer sends its articles in batches to
    the sorters based on count ranges provided by the coordinator.
    """

    def __init__(self, reducer_no, verbose=False):
        self.reducer_no = reducer_no
        self.verbose = verbose
        self.counts = Counter()
        #self.count_df = None
        self.reduce_calls = 0
        self.reduce_calls_since_print = 0
        self.finished_mappers = set() # used in no flow control scenario

    def get_host(self):
        import socket
        return socket.gethostname()

    @ray.method(num_returns=1)
    def reduce(self, other_counter: Counter):
        """Version of reduce where the caller will wait for responses before sending the next batch"""
        for (article, count) in other_counter.items():
            self.counts[article] += count
        self.reduce_calls += 1
        if self.verbose or (self.reduce_calls%REDUCE_PRINT_FREQUENCY)==0:
            print(
                f"Reducer[{self.reducer_no}]: {self.reduce_calls} reductions, {len(self.counts)} pages"
            )

    @ray.method(num_returns=0)
    def reduce_no_flow_control(self, mapper_id: int, other_counter: Counter):
        """Version of reduce where the mapper will not wait for responses before sending the next
        batch. The mapper should call end_of_reduce_calls() to tell this reducer that it is done."""
        assert mapper_id not in self.finished_mappers
        for (article, count) in other_counter.items():
            self.counts[article] += count
        self.reduce_calls += 1
        if self.verbose or (self.reduce_calls%REDUCE_PRINT_FREQUENCY)==0:
            print(
                f"Reducer[{self.reducer_no}]: {self.reduce_calls} reductions, {len(self.counts)} pages"
            )

    # def _get_count_df(self):
    #     if self.count_df is None:
    #         self.count_df = pd.DataFrame(self.counts.items(), columns=["page", "incoming_references"])
    #         self.count_df.sort_values(
    #             by=["incoming_references", "page"], ascending=[False, True], inplace=True
    #         )
    #     return self.count_df

    @ray.method(num_returns=1)
    def end_of_reduce_calls(self, mapper_id:int):
        """Used to signal end of calls for no flow control case"""
        assert mapper_id not in self.finished_mappers
        self.finished_mappers.add(mapper_id)
        return True

    def get_count_distribution(self, num_sorters: int):
        """Determine quantiles for the counts held by this reducer and
        return an ordered list with the bounaries. This list is of length
        num_sorters - 1, unless there are not enough unique counts for
        that many buckets. In that cae, our approach returns just the
        unique counts.
        """
        #counts = self._get_count_df()['incoming_references']
        counts = pd.Series(self.counts.values())
        return sorted(
            counts.quantile([i / num_sorters for i in range(1, num_sorters)]).unique()
        )

    def send_to_sorters(self, sorters, sorter_limits):
        assert len(sorters) == (
            len(sorter_limits) + 1
        ), f"There should be one more sorter than sorter limits, but got {len(sorters)} sorters and {len(sorter_limits)} limits"
        # df = self._get_count_df()
        # split_data = [
        #     df[df['incoming_references']<limit]
        #     for limit in sorter_limits
        # ]
        # split_data.append(df[df['incoming_references']>=sorter_limits[-1]])

        split_data = [
            {"page": [], "incoming_references": []} for i in range(len(sorters))
        ]
        for (article, count) in self.counts.items():
            found = False
            for (sorter_no, limit) in enumerate(sorter_limits):
                if count <= limit:
                    split_data[sorter_no]["page"].append(article)
                    split_data[sorter_no]["incoming_references"].append(count)
                    found = True
                    break
            if not found:
                # if past the limits, put in the highest bucket
                split_data[-1]["page"].append(article)
                split_data[-1]["incoming_references"].append(count)

        futures = [
            sorter.accept_counts.remote(pd.DataFrame(split_data[sorter_no]))
            #sorter.accept_counts.remote(split_data[sorter_no])
            for (sorter_no, sorter) in enumerate(sorters)
        ]
        ray.get(futures)  # wait for the sends to complete

    def __str__(self):
        return str(
            {article: self.counts[article] for article in sorted(self.counts.keys())}
        )


@ray.remote
class Sorter:
    """Each sorter is sent articles with counts that fall within a specified range. When asked by the
    coordinator, it returns a sorted dataframe of its articles and counts.
    """

    def __init__(self, verbose=False):
        self.verbose = verbose
        self.count_dataframes = []

    def get_host(self):
        import socket
        return socket.gethostname()

    def accept_counts(self, data):
        self.count_dataframes.append(data)

    def get_sorted_values(self):
        df = pd.concat(self.count_dataframes)
        df.sort_values(
            by=["incoming_references", "page"], ascending=[False, True], inplace=True
        )
        df.set_index("page", drop=True, inplace=True)
        df["incoming_references"] = df["incoming_references"].astype(np.int32)
        return df


def get_worker_count_and_placement_groups(skip_placement_groups):
    """The size of an individual resource request is limited by the size of the smallest node in
    the cluster. For example, if there are two nodes with 16 cpus and one node with 8 cpus, and you
    request two nodes with 16 cpus, the request will block. Thus we find the size of the
    smallest node in the cluster (ignoring any with 0 cpus) and divide that between the mapper
    and reducer stages. For our example, we will get placement groups with 3 nodes of 4 cpus each.
    """
    nodes = [node for node in ray.nodes() if node['Alive'] and 'CPU' in node['Resources'] and node['Resources']['CPU']>0]
    smallest_cpus = min([node['Resources']['CPU'] for node in nodes])
    #bundle_size = int(smallest_cpus/2)
    bundle_size = int(smallest_cpus)
    total_workers_per_stage = len(nodes)*bundle_size
    if skip_placement_groups:
        print(f"Will use {total_workers_per_stage} workers across {len(nodes)} nodes, skipping placement groups.")
        return (total_workers_per_stage, None, None)
    print(f"Each placement group will have {bundle_size} cpus times {len(nodes)} nodes for {total_workers_per_stage} total workers.")
    mapper_bundle = [{'CPU':bundle_size} for node in nodes]
    reducer_bundle = [{'CPU':bundle_size} for node in nodes]
    print(f"Mapper placement group: {mapper_bundle}")
    mpg = ray.util.placement_group(mapper_bundle, strategy='STRICT_SPREAD')
    ray.get(mpg.ready())
    print("  successfully obtained mapper group.")
    print(f"Reducer placement group: {reducer_bundle}")
    rpg = ray.util.placement_group(reducer_bundle, strategy='STRICT_SPREAD')
    ray.get(rpg.ready())
    print("  successfully obtained reducer group.")
    return (total_workers_per_stage, mpg, rpg)

def get_hostnames(actor_list):
    """Call the get_host() method on the list of actors and return the counts by host"""
    return Counter(ray.get([actor.get_host.remote() for actor in actor_list]))


def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--redis-password",
        default=None,
        help="Password to use for Redis, if non-default",
    )
    parser.add_argument(
        "--address", default='auto', type=str, help="Address for this Ray node, defaults to 'auto'"
    )
    parser.add_argument(
        "--articles-per-mapper-batch",
        type=int,
        default=1000,
        help="Number of articles to read from dump file in each mapper batch, defaults to 1000",
    )
    parser.add_argument(
        "--skip-placement-groups",
        default=False,
        action="store_true",
        help="If specified, don't use placement groups"
    )
    parser.add_argument(
        "--no-flow-control",
        default=False,
        action="store_true",
        help="If specified, mappers will send all their reduce requests without waiting for responses."
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Is specified, print extra debugging",
    )
    parser.add_argument(
        "dump_file",
        metavar="DUMP_FILE",
        type=str,
        help="Name of file containing wikipedia dump. This must be in the same location across"
        + " all the nodes of the Ray cluster.",
    )
    parser.add_argument(
        "output_file",
        metavar="OUTPUT_FILE",
        type=str,
        help="Name to use for output csv file",
    )
    args = parser.parse_args(argv)
    args.dump_file = abspath(expanduser(args.dump_file))
    if not exists(args.dump_file):
        parser.error(f"Did not find input file {args.dump_file}")
    print("Initializing Ray...")
    if args.redis_password is not None:
        ray.init(address=args.address, _redis_password=args.redis_password)
    else:
        ray.init(address=args.address)
    (total_workers_per_stage, mpg, rpg) = get_worker_count_and_placement_groups(args.skip_placement_groups)
    file_size = os.stat(args.dump_file).st_size
    block_size = int(ceil(file_size / total_workers_per_stage))
    print(
        f"File size is {file_size}, which will yield {total_workers_per_stage} blocks of size {block_size}"
    )
    reducers = [
        Reducer.options(placement_group=rpg).remote(r, verbose=args.verbose) for r in range(total_workers_per_stage)
    ]
    mappers = [
        Mapper.options(placement_group=mpg).remote(
            mapper_id,
            args.dump_file,
            reducers,
            args.articles_per_mapper_batch,
            verbose=args.verbose,
        )
        for mapper_id in range(total_workers_per_stage)
    ]
    print(f"{len(mappers)} mappers, {len(reducers)} reducers")
    print(f"Mapper hosts: {get_hostnames(mappers)}")
    print(f"Reducer hosts: {get_hostnames(reducers)}")
    start = time.time()
    if args.no_flow_control:
        mapper_futures = [
            mapper.map_no_flow_control.remote(block_size, block_size * block_no)
            for (block_no, mapper) in enumerate(mappers)
        ]
        print(f"Started {total_workers_per_stage} mappers with no flow control, waiting for completion")
    else:
        mapper_futures = [
            mapper.map.remote(block_size, block_size * block_no)
            for (block_no, mapper) in enumerate(mappers)
        ]
        print(f"Started {total_workers_per_stage} mappers, waiting for completion")
    ray.get(mapper_futures)
    print("Done with mapping")
    mappers = None # make mappers go out of scope
    num_sorters = min(8, total_workers_per_stage)
    quantiles = ray.get(reducers[0].get_count_distribution.remote(num_sorters))
    sorters = [Sorter.options(placement_group=mpg).remote(verbose=args.verbose) for i in range(len(quantiles) + 1)]
    print(f"Sorter hosts: {get_hostnames(sorters)}")
    sort_send_start_time = time.time()
    reducer_futures = [
        reducer.send_to_sorters.remote(sorters, quantiles) for reducer in reducers
    ]
    ray.get(reducer_futures)
    print(f"Done with sending to sorters (took {int(round(time.time()-sort_send_start_time))} seconds)")
    reducers = None # make reducers go out of scope
    print("Writing to file")
    write_header = True
    total_rows = 0
    write_start_time = time.time()
    for sorter_no in range(len(sorters) - 1, -1, -1):
        df = ray.get(sorters[sorter_no].get_sorted_values.remote())
        df.to_csv(
            args.output_file, header=write_header, mode="w" if write_header else "a"
        )
        write_header = False
        total_rows += len(df)
    print(f"Wrote {total_rows} rows to {args.output_file} in {int(round(time.time()-write_start_time))} seconds")

    end = time.time()
    elapsed = end - start
    if elapsed >= 1.0:
        print(f"Completed in {int(round(elapsed))} seconds")
    else:
        print(f"Completed in {round(elapsed, 2)} seconds")
    return 0


if __name__ == "__main__":
    sys.exit(main())
