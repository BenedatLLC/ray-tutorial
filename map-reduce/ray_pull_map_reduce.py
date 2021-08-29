#!/usr/bin/env python3
"""This program implements a parallel map-reduce-sort for
building a sorted list of Wikipedia articles and their inbound
reference counts. It uses a Ray actor class for mappers and reducers.
The sort is done in the driver program. Mappers and reducers may have
multiple workers,
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
from typing import TextIO, Generator, Tuple, Set, Optional, NamedTuple, List, cast

import ray
from ray.util.placement_group import PlacementGroup
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


@ray.remote(num_cpus=0.5)
class Mapper:
    """Each mapper is an actor that waits for a map request. Upon the first map request,
    it reads the entire file and updates a counter for each reducer. These are cached in
    the mapper and then returned based on the reducer requesting the data.
    """

    def __init__(
        self,
        mapper_id: int,
        dump_file: str,
        offset: int,
        block_size: int,
        num_reducers: int,
        verbose: bool = False,
    ):
        self.mapper_id = mapper_id
        assert exists(
            dump_file
        ), f"Mapper did not find dump file {dump_file}. Is it on the same path for all nodes?"
        self.dump_file = dump_file
        self.block_size = block_size
        self.offset = offset
        self.num_reducers = num_reducers
        self.counters = None  # type: Optional[List[Optional[Counter]]]
        self.verbose = verbose

    def get_host(self):
        import socket

        return socket.gethostname()

    def map(self, reducer_no: int) -> Counter:
        if self.counters is not None:
            # have already retrieved the data
            counter = self.counters[reducer_no]
            assert counter is not None
            self.counters[reducer_no] = None
            return counter
        else:  # need to read from the file
            self.counters = [Counter() for c in range(self.num_reducers)]

            with open(self.dump_file, "r") as f:
                for (article, references) in read_block(
                    f, self.block_size, self.offset, verbose=self.verbose
                ):
                    for ref_article in references:
                        cast(
                            Counter,
                            self.counters[get_reducer(ref_article, self.num_reducers)],
                        )[ref_article] += 1

            print(f"Mapper {self.mapper_id} completed read of wiki data")
            # now, return the counter for the requesting reducer
            counter = self.counters[reducer_no]
            assert counter is not None
            self.counters[reducer_no] = None
            return counter


REDUCE_PRINT_FREQUENCY = 10000


def get_num_returns(futures, fraction):
    """given a list of futures and the value for"""
    num_futures = len(futures)
    if fraction is None:
        return 1
    elif fraction == 100:
        return num_futures
    else:
        nr = int(round(fraction / 100 * len(futures)))
        return min(max(nr, 1), num_futures)


@ray.remote(num_cpus=0.5)
class Reducer:
    """Reducers request article counts from each of the mappers. A hash algorithm
    ensures that a given article is only mapped to a specific reducer. Thus, the
    reducer can compute the counts for the subset of wikipedia that it is responsible
    for. Once it has all the data from the mappers, it converts the counter to a data
    frame and sorts it before returning.
    """

    def __init__(self, reducer_no, pct_pending_requests, verbose=False):
        self.reducer_no = reducer_no
        self.pct_pending_requests = pct_pending_requests
        self.verbose = verbose

    def get_host(self):
        import socket

        return socket.gethostname()

    def reduce(self, mappers) -> pd.DataFrame:
        counts = Counter()  # type: Counter
        futures = [mapper.map.remote(self.reducer_no) for mapper in mappers]
        while len(futures) > 0:
            ready_futures, remaining_futures = ray.wait(
                futures, num_returns=get_num_returns(futures, self.pct_pending_requests)
            )
            for future in ready_futures:
                counter = ray.get(future)
                for (article, count) in counter.items():
                    counts[article] += count
            futures = remaining_futures
        print(f"Reducer[{self.reducer_no}] has completed map calls.")

        df = pd.DataFrame(counts.items(), columns=["page", "incoming_references"])
        df["incoming_references"] = df["incoming_references"].astype(np.int32)
        df.sort_values(
            by=["incoming_references", "page"], ascending=[False, True], inplace=True
        )
        return df


def sort(mappers, reducers, pct_pending_requests) -> pd.DataFrame:
    """We run a single sort function on the driver node that requests dataframes from each of the reducers.
    It then merges the dataframes and performs a global sort.
    """
    import socket

    print(f"Sorter is on host {socket.gethostname()}")
    futures = [reducer.reduce.remote(mappers) for reducer in reducers]
    data_frames = []  # type: Optional[List[pd.DataFrame]]
    while len(futures) > 0:
        ready_futures, remaining_futures = ray.wait(
            futures, num_returns=get_num_returns(futures, pct_pending_requests)
        )
        for future in ready_futures:
            cast(List[pd.DataFrame], data_frames).append(ray.get(future))
        futures = remaining_futures
    print("All reducers have completed, starting sort...")
    sort_start = time.time()
    df = pd.concat(data_frames)
    data_frames = None
    df.sort_values(
        by=["incoming_references", "page"], ascending=[False, True], inplace=True
    )
    df.set_index("page", drop=True, inplace=True)
    # df["incoming_references"] = df["incoming_references"].astype(np.int32)
    print(f"Sort completed in {int(round(time.time()-sort_start))} seconds")
    return df


class PlacementInfo(NamedTuple):
    num_worker_nodes: int
    total_workers_per_stage: int
    mapper_placement_group: Optional[PlacementGroup]
    reducer_placement_group: Optional[PlacementGroup]


def get_worker_count_and_placement_groups(skip_placement_groups) -> PlacementInfo:
    """The size of an individual resource request is limited by the size of the smallest node in
    the cluster. For example, if there are two nodes with 16 cpus and one node with 8 cpus, and you
    request two nodes with 16 cpus, the request will block. Thus we find the size of the
    smallest node in the cluster (ignoring any with 0 cpus). We use the same placement group for
    mappers and reducers. If skip_placement_groups is True, then we just
    """
    nodes = [
        node
        for node in ray.nodes()
        if node["Alive"] and "CPU" in node["Resources"] and node["Resources"]["CPU"] > 0
    ]
    total_cpus = sum([int(node["Resources"]["CPU"]) for node in nodes])
    if skip_placement_groups:
        return PlacementInfo(len(nodes), total_cpus, None, None)
    smallest_cpus = int(min([node["Resources"]["CPU"] for node in nodes]))
    total_workers_per_stage = int(smallest_cpus * len(nodes))
    bundle = [{"CPU": smallest_cpus} for node in nodes]
    print(f"Placement group: {bundle}")
    pg = ray.util.placement_group(bundle, strategy="STRICT_SPREAD")
    ray.get(pg.ready())
    print(f" obtained placement group: {ray.util.placement_group_table(pg)}")
    return PlacementInfo(len(nodes), total_workers_per_stage, pg, pg)


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
        "--address",
        default="auto",
        type=str,
        help="Address for this Ray node, defaults to 'auto'",
    )
    parser.add_argument(
        "--skip-placement-groups",
        default=False,
        action="store_true",
        help="If specified, don't use placement groups",
    )
    parser.add_argument(
        "--pct-pending-requests",
        type=int,
        default=50,
        help="Fraction of pending requests to wait for, as a percentage of outstanding requests. If not specified, will wait for 50% of the outstanding requests",
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
    placement = get_worker_count_and_placement_groups(args.skip_placement_groups)
    file_size = os.stat(args.dump_file).st_size
    block_size = int(ceil(file_size / placement.total_workers_per_stage))
    print(
        f"File size is {file_size}, which will yield {placement.total_workers_per_stage} blocks of size {block_size}"
    )
    mappers = [
        Mapper.options(
            placement_group=placement.mapper_placement_group,
            placement_group_bundle_index=mapper_id % placement.num_worker_nodes,
        ).remote(
            mapper_id,
            args.dump_file,
            mapper_id * block_size,
            block_size,
            placement.total_workers_per_stage,
            verbose=args.verbose,
        )
        for mapper_id in range(placement.total_workers_per_stage)
    ]
    reducers = [
        Reducer.options(
            placement_group=placement.reducer_placement_group,
            placement_group_bundle_index=r % placement.num_worker_nodes,
        ).remote(
            r, pct_pending_requests=args.pct_pending_requests, verbose=args.verbose
        )
        for r in range(placement.total_workers_per_stage)
    ]
    print(f"{len(mappers)} mappers, {len(reducers)} reducers")
    print(f"Mapper hosts: {get_hostnames(mappers)}")
    print(f"Reducer hosts: {get_hostnames(reducers)}")

    start = time.time()

    df = sort(mappers, reducers, args.pct_pending_requests)
    # sorter_future = sort.remote(mappers, reducers, args.pct_pending_requests)
    # print("Called sorter, waiting for completion")
    # df = ray.get(sorter_future)

    start_write = time.time()
    df.to_csv(args.output_file, header=True, mode="w")
    print(
        f"Wrote {len(df)} rows to {args.output_file} in {int(round(time.time()-start_write, 1))} seconds"
    )

    end = time.time()
    elapsed = end - start
    print(f"Completed in {int(round(elapsed))} seconds")
    return 0


if __name__ == "__main__":
    sys.exit(main())
