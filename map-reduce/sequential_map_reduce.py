#!/usr/bin/env python3
"""This program sequential builds a sorted list of Wikipedia articles and their
inbound reference counts.
"""
import re
import sys
import argparse
import os
from os.path import abspath, expanduser, exists
from collections import Counter
import csv
import time
import datetime
from typing import TextIO, Generator, Tuple, Set, Optional
import typing # needed to get Counter, which conflicts with collections.Counter

DEFAULT_BLOCK_SIZE = 4096

# Regular expresssion to look for titles and references
PATTERN=r'(?:<title>.+?</title>)|(?:\[\[.+?\]\])'
RE=re.compile(PATTERN)


def read_lines(f:TextIO, verbose:bool=False) \
    -> Generator[Tuple[str, Set[str]], None, None]:
    """Read through the lines of the input Wikipedia dump
    file, generating a stream of article names with the set
    of articles they reference. This is the "map" part of the
    map reduce.
    """
    current_page :Optional[str] = None
    references : Set[str] = set()
    for line in f:
        matches = RE.findall(line)
        for match in matches:
            if match.startswith('<ti'):
                new_page = match[7:-8] # get the text within the title
                if current_page is None:
                    current_page = new_page
                else:
                    yield(current_page, references)
                    current_page = new_page
                    references = set()
            else:
                assert match.startswith('[[')
                if current_page is None:
                    continue # we skip until we reach a title
                body = match[2:-2]
                if ':' in body: # we skip the special tags
                    if verbose:
                        print(f"skipping {body}")
                    continue
                references.add(body.split('|')[0].strip())
    # reached the end of the file
    if current_page:
        yield (current_page, references)

def reduce(article:str, references:Set[str], counter:typing.Counter[str]) -> None:
    """Our reduce is simple: go through each of the references from an article
    and increment their counts in a global counter."""
    for reference in references:
        counter[reference] += 1


def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser()
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
    print(f"File size is {file_size} bytes")
    start = time.time()
    print(f"Starting at {datetime.datetime.now().isoformat()}")
    counter = Counter()
    print_interval = 1000 if args.verbose else 100000
    with open(args.dump_file) as f:
        articles_processed = 0
        references_processed = 0
        for (article, references) in read_lines(f, verbose=args.verbose):
            reduce(article, references, counter)
            references_processed += len(references)
            articles_processed += 1
            if (articles_processed%print_interval)==0:
                print(f"{datetime.datetime.now().isoformat()} Processed {articles_processed} articles, {references_processed} references.")
    print(f"{datetime.datetime.now().isoformat()} Processed {articles_processed} articles, {references_processed} references.")
    print("Finished scanning, will sort and write to file.")
    with open(args.output_file, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['page', 'incoming_references'])
        for (page, cnt) in sorted(counter.items(), key=lambda item:(-item[1], item[0])):
            writer.writerow([page, cnt])
    print(f"Wrote {len(counter)} entries.")
    end = time.time()
    elapsed = end-start
    if elapsed>=1.0:
        print(f"Completed in {int(round(elapsed))} seconds")
    else:
        print(f"Completed in {round(elapsed, 2)} seconds")
    return 0


if __name__=='__main__':
    sys.exit(main())
