#!/usr/bin/env python3
"""
A simple script to run a Python script in the cluster via
"docker exec".
The script is assumed to be under the root of this repo (ray-tutorial)
and the path is translated to be under /host in the container.

USAGE:
  run_in_cluster SCRIPT_FILE [ARG_TO_SCRIPT] ...
"""

import sys
from os.path import abspath, expanduser, commonpath, join, dirname
import subprocess


def main(argv=sys.argv[1:]):
    if len(argv)<1:
        print("{sys.argv[0]}: missing script to run", file=sys.stderr)
        print("USAGE: {sys.argv[0]} SCRIPT_FILE [ARG_TO_SCRIPT] ...", file=sys.stderr)
        return 1
    script_path = abspath(expanduser(argv[0]))
    script_args = argv[1:]
    base_path = abspath(expanduser(join(dirname(__file__), '..')))
    print(f"base path is {base_path}")
    if commonpath([script_path, base_path])!=base_path:
        parser.error(f"Script {script_path} is not in a subdirectory of {base_path}")
    path_in_container = join('/host', script_path[len(base_path)+1:])
    print(f"path_in container is {path_in_container}")
    cmd = f"docker exec -it ray-head-container /root/anaconda3/bin/python {path_in_container}"
    if len(script_args)>0:
        cmd += " " + ' '.join(script_args)
    print(f"Running command: {cmd}")
    subprocess.run(cmd, shell=True)
    return 0

if __name__=='__main__':
    sys.exit(main())
