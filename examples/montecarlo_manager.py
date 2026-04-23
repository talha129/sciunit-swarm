#!/usr/bin/env python3
"""
Monte Carlo pi estimation via TaskVine PythonTask.

Each task throws N random darts and returns the count inside the unit circle.
Manager aggregates all results and prints the final pi estimate.

Usage:
    python3 montecarlo_manager.py [--port 9123] [--tasks 20] [--samples 1000000]

Worker command for sciunit-swarm exec:
    "vine_worker {manager_host} 9123"
"""

import argparse
import socket

import ndcctools.taskvine as vine


def monte_carlo(n_samples):
    import numpy as np
    pts = np.random.rand(n_samples, 2)
    return int(np.sum((pts ** 2).sum(axis=1) <= 1.0))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port',    type=int, default=9123)
    parser.add_argument('--tasks',   type=int, default=20)
    parser.add_argument('--samples', type=int, default=1_000_000)
    args = parser.parse_args()

    m = vine.Manager(port=args.port)
    print(f"[manager] host={socket.gethostname()} port={m.port}")
    print(f"[manager] tasks={args.tasks}  samples/task={args.samples:,}")

    for _ in range(args.tasks):
        t = vine.PythonTask(monte_carlo, args.samples)
        t.set_cores(1)
        m.submit(t)

    total_inside  = 0
    total_samples = 0

    while not m.empty():
        t = m.wait(5)
        if t:
            if isinstance(t.output, vine.PythonTaskNoResult):
                print(f"  task {t.id:3d} failed")
                continue
            inside = t.output
            total_inside  += inside
            total_samples += args.samples
            pi_est = 4 * total_inside / total_samples
            print(f"  task {t.id:3d} inside={inside:>9,}  running pi={pi_est:.6f}")

    pi_final = 4 * total_inside / total_samples
    print(f"\n[manager] total samples : {total_samples:,}")
    print(f"[manager] pi estimate   : {pi_final:.6f}")
    print(f"[manager] error         : {abs(pi_final - 3.14159265358979):.6f}")


if __name__ == '__main__':
    main()
