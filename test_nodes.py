#!/usr/bin/env python3
"""
Example from the Ray docs to get the list of nodes in the cluster
"""
import time
import sys
import ray
import ray.util
import argparse


@ray.remote
def f(sleep_time):
    time.sleep(sleep_time)
    return ray.util.get_node_ip_address()

def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser()
    parser.add_argument('--address', default='auto', type=str,
                        help="Address for this Ray node")
    parser.add_argument('--sleep-time', default=0.01, type=float,
                        help="Time to sleep in seconds (or a fraction of a second.)"+
                        " Defaults to 0.01 seconds.")
    parser.add_argument('--num-actors', type=int, default=100,
                        help="number of actors to spawn, defaults to 100")
    parser.add_argument('redis_password', metavar="REDIS_PASSWORD", default=None,
                        help="Password to use for Redis")
    args = parser.parse_args(argv)

    print("Initializing Ray...")
    if args.redis_password is not None:
        ray.init(address=args.address, _redis_password=args.redis_password)
    else:
        ray.init(address=args.address)

    nodes = ray.nodes()
    print("Nodes:")
    for node in nodes:
        print(f"  {node['NodeManagerHostname']}") 
    print()
    bundles = [{'CPU':1} for node in nodes]
    pg = ray.util.placement_group(bundles, strategy='SPREAD')
    print(f"Getting placement group with {len(bundles)} bundles...")
    print(ray.get(pg.ready()))
    g = f.options(placement_group=pg)
    print("Running tasks...")
    # Get a list of the IP addresses of the nodes that have joined the cluster.
    refs = [g.remote(args.sleep_time) for _ in range(args.num_actors)]
    ips = set()
    fetch_size = max(int(args.num_actors/10), 1)
    # we fetch up to 1/10th of the total each time
    while True:
        ready_refs, remaining_refs = ray.wait(refs, num_returns=fetch_size, timeout=5)
        ips = ips.union(set(ray.get(ready_refs)))
        print(f"Found the following IPs: {', '.join(ips)}")
        if len(remaining_refs)>0:
            print(f"Waiting for {len(remaining_refs)} actors to finish")
            refs = remaining_refs
            time.sleep(2)
        else:
            break
    print("Completed successfully.")

if __name__ == '__main__':
    sys.exit(main())
