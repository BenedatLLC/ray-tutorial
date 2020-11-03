"""
This is a translation of the Apache Spark Pi estimation demo
(https://spark.apache.org/examples.html) to Ray.
"""
import random
import argparse
import sys

import ray

@ray.remote
def inside():
    x, y = random.random(), random.random()
    return x*x + y*y < 1

def get_estimate(num_samples):
    print("Will submit %d samples" % num_samples)
    futures = [inside.remote() for i in range(num_samples)]
    print("Submitted, waiting for responses...")
    
    print(f"Estimate of Pi with {num_samples} samples is {4*sum(ray.get(futures))/num_samples}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--redis-password', default=None,
                        help="Password to use for Redis, if non-default")
    parser.add_argument('--address', default='auto', type=str,
                        help="Address for this Ray node")
    parser.add_argument('num_samples', metavar='NUM_SAMPLES', nargs='?',
                        default=1000, type=int,
                        help="Number of samples to obtain in parallel")
    args = parser.parse_args()
    print("Initializing Ray...")
    if args.redis_password is not None:
        ray.init(address=args.address, _redis_password=args.redis_password)
    else:
        ray.init(address=args.address)
    get_estimate(args.num_samples)

if __name__=='__main__':
    main()
