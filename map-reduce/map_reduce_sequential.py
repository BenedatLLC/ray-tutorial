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

DEFAULT_BLOCK_SIZE = 4096

PATTERN=r'(?:<title>.+?</title>)|(?:\[\[.+?\]\])'
RE=re.compile(PATTERN)

class ReadState(Enum):
    INITIAL = 0
    READING_BODY = 1
    READING_UNTIL_NEXT_TITLE = 2

def line_reader(f, offset):
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

        
def read_block(f, block_size, offset=0, verbose=False):
    if verbose:
        print(f"read_block({offset})")
    if offset!=0:
        reader = line_reader(f, offset)
    else:
        reader = f
    state = ReadState.INITIAL
    current_page = None
    references = set()
    chars_read = 0
    first_line = True
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
                    yield(current_page, references)
                    current_page = new_page
                    references = set()
                else:
                    assert state==ReadState.READING_UNTIL_NEXT_TITLE
                    if verbose:
                        print(f"Hit next title {new_page}, yielding {current_page}")
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

def mapper(f, block_size, offset, verbose=False):
    c = Counter()
    for (article, references) in read_block(f, block_size, offset, verbose=verbose):
        for ref_article in references:
            c[ref_article]+= 1
    return c

class Reducer:
    def __init__(self, verbose=False):
        self.verbose=verbose
        self.counts = Counter()
        self.reduce_calls = 0
        self.reduce_calls_since_print = 0
    def reduce(self, other_counter):
        self.counts += other_counter
        self.reduce_calls += 1
        self.reduce_calls_since_print += 1
        if self.verbose or self.reduce_calls_since_print>100:
            print(f"Reducer: {self.reduce_calls} reductions, {len(self.counts)} pages")
            self.reduce_calls_since_print = 0
    def __str__(self):
        return str({article:self.counts[article] for article in sorted(self.counts.keys())})
    def to_csv(self, filename):
        print(f"Writing output to {filename}")
        with open(filename, 'w') as f:
            writer = csv.writer(f)
            writer.writerow(['page', 'incoming_references'])
            for (page, cnt) in sorted(self.counts.items(), key=lambda item:(-item[1], item[0].lower())):
                writer.writerow([page, cnt])
        print(f"Wrote {len(self.counts)} entries.")



def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser()
    parser.add_argument("--block-size", type=int, default=DEFAULT_BLOCK_SIZE,
                        help=f"Size of blocks to read from dump file, defaults to {DEFAULT_BLOCK_SIZE}")
    parser.add_argument('--max-blocks', type=int, default=None,
                        help="Maximum number of blocks to read (defaults to all)")
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
    file_size = os.stat(args.dump_file).st_size
    block_count = int(ceil(file_size/args.block_size))
    print(f"File size is {file_size}, which will yield {block_count} blocks")
    reducer = Reducer(verbose=args.verbose)
    blocks_to_read = min(block_count, args.max_blocks) if args.max_blocks \
                     else block_count
    start = time.time()
    with open(args.dump_file) as f:
        for blockno in range(blocks_to_read):
            c = mapper(f, args.block_size, args.block_size*blockno, verbose=args.verbose)
            reducer.reduce(c)
    reducer.to_csv(args.output_file)
    end = time.time()
    elapsed = end-start
    if elapsed>=1.0:
        print(f"Completed in {int(round(elapsed))} seconds")
    else:
        print(f"Completed in {round(elapsed, 2)} seconds")
    return 0


if __name__=='__main__':
    sys.exit(main())
