"""
End-to-end scripted demo.

  1. Boots 3 backend servers + 1 load balancer as real subprocesses.
  2. Runs a baseline load test (all backends healthy).
  3. Kills one backend process mid-run to simulate a node failure, then
     re-runs the load test to prove the load balancer detects it and
     fails over with no client-visible outage.
  4. Runs a burst test well above the configured rate limit to prove the
     limiter actually throttles clients (watch the 429 rate).

Run with: python3 run_demo.py
"""

import subprocess
import sys
import time
import os

from load_test import run_load_test

BACKEND_PORTS = [8001, 8002, 8003]
LB_PORT = 9000
HERE = os.path.dirname(os.path.abspath(__file__))


def start_backend(port):
    return subprocess.Popen(
        [sys.executable, "backend_server.py", "--port", str(port)],
        cwd=HERE,
    )


def start_lb(algorithm="least_connections"):
    backends_spec = ",".join(f"127.0.0.1:{p}" for p in BACKEND_PORTS)
    return subprocess.Popen(
        [sys.executable, "load_balancer.py",
         "--port", str(LB_PORT),
         "--backends", backends_spec,
         "--algorithm", algorithm,
         "--rate-capacity", "30",
         "--rate-refill", "15"],
        cwd=HERE,
    )


def main():
    print("Starting 3 backend servers on", BACKEND_PORTS)
    backends = [start_backend(p) for p in BACKEND_PORTS]
    time.sleep(0.6)

    print("Starting load balancer on port", LB_PORT)
    lb = start_lb()
    time.sleep(1.0)

    try:
        # Phase 1: 40 distinct simulated clients, each well within their
        # individual rate budget (30 burst / 15 per sec).
        run_load_test(
            "127.0.0.1", LB_PORT, concurrency=40, requests_per_client=25,
            label="PHASE 1: baseline, 3/3 backends healthy, 40 distinct clients",
        )

        print(f"\n>>> Killing backend on port {BACKEND_PORTS[0]} to simulate a node failure...")
        backends[0].terminate()
        backends[0].wait()
        time.sleep(1.2)  # let the health-check loop notice

        run_load_test(
            "127.0.0.1", LB_PORT, concurrency=40, requests_per_client=25,
            label="PHASE 2: after node failure, 2/3 backends healthy (failover)",
        )

        # Phase 3: one single client IP firing far more requests than its
        # bucket allows, to prove the limiter throttles the offender...
        run_load_test(
            "127.0.0.1", LB_PORT, concurrency=1, requests_per_client=100,
            label="PHASE 3a: single abusive client, 100 rapid-fire requests",
            distinct_clients=False,
        )

        # ...while 20 other well-behaved distinct clients are completely
        # unaffected, proving per-client isolation (not a global limit).
        run_load_test(
            "127.0.0.1", LB_PORT, concurrency=20, requests_per_client=5,
            label="PHASE 3b: 20 other well-behaved clients, unaffected",
        )

    finally:
        print("\nShutting down...")
        lb.terminate()
        for b in backends:
            if b.poll() is None:
                b.terminate()


if __name__ == "__main__":
    main()
