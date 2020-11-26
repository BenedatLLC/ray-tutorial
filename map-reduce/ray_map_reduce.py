import re
import sys
from enum import Enum
import argparse
import os
from os.path import abspath, expanduser, exists
from math import ceil
from collections import Counter
import csv
import time
from typing import TextIO, Generator, Tuple, Set, Optional

import ray
import numpy as np
import pandas as pd

DEFAULT_BLOCK_SIZE = 4096

PATTERN=r'(?:<title>.+?</title>)|(?:\[\[.+?\]\])'
RE=re.compile(PATTERN)

class ReadState(Enum):
    INITIAL = 0
    READING_BODY = 1
    READING_UNTIL_NEXT_TITLE = 2

def line_reader(f:TextIO, offset:int):
    for i in range(4):
        f.seek(offset+i)
        try:
            yield next(f)
            break
        except UnicodeDecodeError as e:
            if i==3:
                raise
            else:
                print(f"Got decode error {e} at offset {offset}, will try next byte")
    for line in f:
        yield line

        
def read_block(f:TextIO, block_size:int, offset:int=0, verbose:bool=False) \
    -> Generator[Tuple[str, Set[str]], None, None]:
    if verbose:
        print(f"read_block(offset={offset}, size={block_size})")
    if offset!=0:
        reader = line_reader(f, offset)
    else:
        reader = f
    state = ReadState.INITIAL
    current_page :Optional[str] = None
    references : Set[str] = set()
    chars_read = 0
    for line in reader:
        chars_read += len(line)
        matches = RE.findall(line)
        for match in matches:
            if match.startswith('<ti'):
                new_page = match[7:-8]
                if state==ReadState.INITIAL:
                    current_page = new_page
                    state = ReadState.READING_BODY
                elif state==ReadState.READING_BODY:
                    assert current_page is not None
                    yield(current_page, references)
                    current_page = new_page
                    references = set()
                else:
                    assert state==ReadState.READING_UNTIL_NEXT_TITLE
                    if verbose:
                        print(f"Hit next title {new_page}, yielding {current_page}")
                    assert current_page is not None
                    yield (current_page, references)
                    return # we hit the first title of the next block
            else:
                assert match.startswith('[[')
                if state==ReadState.INITIAL:
                    continue # we skip until we reach a title
                body = match[2:-2]
                if ':' in body: # we skip the special tags
                    if verbose:
                        print(f"skipping {body}")
                    continue
                references.add(body.split('|')[0].strip())
        if chars_read>=block_size:
            if state==ReadState.INITIAL:
                if verbose:
                    print("did not find anything in block")
                return # did not find anything in this block
            elif state==ReadState.READING_BODY:
                state = ReadState.READING_UNTIL_NEXT_TITLE
                if verbose:
                    print(f"hit end of page, but still reading article {current_page}")
    # reached the end of the file
    if current_page:
        yield (current_page, references)

def get_reducer(article:str, num_reducers:int) -> int:
    """We map an article title to a reducer. This is done via hashing."""
    return (sum([ord(s) for s in article])+len(article))%num_reducers

@ray.remote
class Mapper:
    def __init__(self, dump_file:str, reducers, articles_per_batch:int, verbose:bool=False):
        self.dump_file = dump_file
        assert exists(dump_file), f"Mapper did not find dump file {dump_file}. Is it on the same path for all nodes?"
        self.reducers = reducers # an array of reducers
        self.articles_per_batch = articles_per_batch
        self.verbose=verbose


    def map(self, block_size, offset):
        reduce_futures = []
        counters = [Counter() for c in range(len(self.reducers))]
        def send_batch(futures):
            if len(futures)>0:
                # we make sure the previous batch of reduce calls have completed
                ray.get(futures)
            new_futures = []
            for (reducer, counter) in enumerate(counters):
                if len(counter)>0:
                    new_futures.append(self.reducers[reducer].reduce.remote(counter))
                    if self.verbose:
                        print(f"Mapper[{offset}] send {len(counter)} to reducer {reducer}")
            return new_futures

        with open(self.dump_file, 'r') as f:
            articles_in_batch = 0
            for (article, references) in read_block(f, block_size, offset, verbose=self.verbose):
                for ref_article in references:
                    counters[get_reducer(ref_article, len(self.reducers))][ref_article] += 1
                articles_in_batch += 1
                if articles_in_batch==self.articles_per_batch:
                    reduce_futures = send_batch(reduce_futures)
                    articles_in_batch = 0
                    counters = [Counter() for c in range(len(self.reducers))]

            if articles_in_batch>0:
                reduce_futures = send_batch(reduce_futures)
            if len(reduce_futures)>0:
                ray.get(reduce_futures)
            print(f"Mapper[{offset}] completed")

@ray.remote
class Reducer:
    def __init__(self, reducer_no, verbose=False):
        self.reducer_no = reducer_no
        self.verbose=verbose
        self.counts = Counter()
        self.reduce_calls = 0
        self.reduce_calls_since_print = 0

    def reduce(self, other_counter:Counter):
        self.counts += other_counter
        self.reduce_calls += 1
        self.reduce_calls_since_print += 1
        if self.verbose or self.reduce_calls_since_print>100:
            print(f"Reducer[{self.reducer_no}]: {self.reduce_calls} reductions, {len(self.counts)} pages")
            self.reduce_calls_since_print = 0

    def get_count_distribution(self, num_sorters:int):
        counts = pd.Series(self.counts.values())
        return sorted(counts.quantile([i/num_sorters for i in range(1, num_sorters)]).unique())

    def send_to_sorters(self, sorters, sorter_limits):
        assert len(sorters)==(len(sorter_limits)+1),\
            f"There should be one more sorter than sorter limits, but got {len(sorters)} sorters and {len(sorter_limits)} limits"
        split_data = [{'page':[], 'incoming_references':[]} for i in range(len(sorters))]
        for (article, count) in self.counts.items():
            found = False
            for (sorter_no, limit) in enumerate(sorter_limits):
                if count<=limit:
                    split_data[sorter_no]['page'].append(article)
                    split_data[sorter_no]['incoming_references'].append(count)
                    found = True
                    break
            if not found:
                # if past the limits, put in the highest bucket
                split_data[-1]['page'].append(article)
                split_data[-1]['incoming_references'].append(count)
                    
        futures = [
            sorter.accept_counts.remote(pd.DataFrame(split_data[sorter_no]))
            for (sorter_no, sorter) in enumerate(sorters)
        ]
        ray.get(futures) # wait for the sends to complete

    def __str__(self):
        return str({article:self.counts[article] for article in sorted(self.counts.keys())})


@ray.remote
class Sorter:
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.count_dataframes = []

    def accept_counts(self, data):
        self.count_dataframes.append(data)

    def get_sorted_values(self):
        df = pd.concat(self.count_dataframes)
        df.sort_values(by=['incoming_references', 'page'], ascending=[False, True], inplace=True)
        df.set_index('page', drop=True, inplace=True)
        df['incoming_references']=df['incoming_references'].astype(np.int32)
        return df


def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser()
    parser.add_argument('--redis-password', default=None,
                        help="Password to use for Redis, if non-default")
    parser.add_argument('--address', default=None, type=str,
                        help="Address for this Ray node")
    parser.add_argument('--num-mappers', default=1, type=int,
                        help="Number of mapper workers")
    parser.add_argument('--num-reducers', default=1, type=int,
                        help="Number of reducer workers")
    parser.add_argument('--num-sorters', default=1, type=int,
                        help="Number of sorter workers")
    parser.add_argument('--articles-per-mapper-batch', type=int, default=10,
                        help="Number of articles to read from dump file in each mapper batch")
    parser.add_argument('--verbose', action='store_true', default=False,
                        help="Is specified, print extra debugging")
    parser.add_argument("dump_file", metavar='DUMP_FILE', type=str,
                        help="Name of file containing wikipedia dump")
    parser.add_argument('output_file', metavar='OUTPUT_FILE', type=str,
                        help="Name to use for output csv file")
    args = parser.parse_args(argv)
    args.dump_file = abspath(expanduser(args.dump_file))
    if not exists(args.dump_file):
        parser.error(f"Did not find input file {args.dump_file}")
    print("Initializing Ray...")
    if args.redis_password is not None:
        ray.init(address=args.address, _redis_password=args.redis_password)
    else:
        ray.init(address=args.address)
    file_size = os.stat(args.dump_file).st_size
    block_size = int(ceil(file_size/args.num_mappers))
    print(f"File size is {file_size}, which will yield {args.num_mappers} blocks of size {block_size}")
    reducers = [Reducer.remote(r, verbose=args.verbose) for r in range(args.num_reducers)]
    mappers = [Mapper.remote(args.dump_file, reducers, args.articles_per_mapper_batch, verbose=args.verbose)
               for r in range(args.num_mappers)]
    print(f"{len(mappers)} mappers, {len(reducers)} reducers")
    start = time.time()
    mapper_futures = [
        mapper.map.remote(block_size, block_size*block_no)
        for (block_no, mapper) in enumerate(mappers)
    ]
    print(f"Started {args.num_mappers}, waiting for completion")
    ray.get(mapper_futures)
    print("Done with mapping")
    for mapper in mappers:
        ray.kill(mapper)
    quantiles = ray.get(reducers[0].get_count_distribution.remote(4))
    sorters = [Sorter.remote(verbose=args.verbose) for i in range(len(quantiles)+1)]
    reducer_futures = [reducer.send_to_sorters.remote(sorters, quantiles) for reducer in reducers]
    ray.get(reducer_futures)
    print("Done with sending to sorters")
    for reducer in reducers:
        ray.kill(reducer)
    print("Writing to file")
    write_header = True
    total_rows = 0
    for sorter_no in range(len(sorters)-1, -1, -1):
        df = ray.get(sorters[sorter_no].get_sorted_values.remote())
        df.to_csv(args.output_file, header=write_header, mode='w' if write_header else 'a')
        write_header = False
        total_rows += len(df)
    print(f"Wrote {total_rows} rows to {args.output_file}")

    end = time.time()
    elapsed = end-start
    if elapsed>=1.0:
        print(f"Completed in {int(round(elapsed))} seconds")
    else:
        print(f"Completed in {round(elapsed, 2)} seconds")
    return 0


if __name__=='__main__':
    sys.exit(main())
