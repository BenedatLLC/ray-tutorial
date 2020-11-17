#!/usr/bin/env python3
"""
Example from the Ray docs to get the list of nodes in the cluster
"""
import time
import sys
import ray
import ray.services
import argparse


@ray.remote
def f(sleep_time):
    time.sleep(sleep_time)
    return ray.services.get_node_ip_address()

def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser()
    parser.add_argument('--redis-password', default=None,
                        help="Password to use for Redis, if non-default")
    parser.add_argument('--address', default=None, type=str,
                        help="Address for this Ray node")
    parser.add_argument('--sleep-time', default=0.01, type=float,
                        help="Time to sleep in seconds (or a fraction of a second.)"+
                        " Defaults to 0.01 seconds.")
    args = parser.parse_args(argv)

    print("Initializing Ray...")
    if args.redis_password is not None:
        ray.init(address=args.address, _redis_password=args.redis_password)
    else:
        ray.init(address=args.address)

    print("Running tasks...")
    # Get a list of the IP addresses of the nodes that have joined the cluster.
    print(set(ray.get([f.remote(args.sleep_time) for _ in range(1000)])))
    print("Completed successfully.")

if __name__ == '__main__':
    sys.exit(main())
