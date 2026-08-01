"""
A minimal concurrent load-testing client, built on raw sockets and threads
(no locust/wrk dependency). Spawns N concurrent "virtual users" hammering
the target, and reports throughput plus p50/p95/p99 latency.
"""

import socket
import time
import threading
import argparse


def make_request(host, port, path="/", timeout=3, source_ip=None):
    """Optionally bind the client socket to a specific source IP so
    concurrent virtual users appear as distinct clients to the rate
    limiter. Linux routes all of 127.0.0.0/8 to loopback, so we can
    simulate many distinct "clients" locally without any real network.
    """
    start = time.perf_counter()
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        if source_ip:
            s.bind((source_ip, 0))
        with s:
            s.connect((host, port))
            req = f"GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n".encode()
            s.sendall(req)
            response = b""
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                response += chunk
        elapsed_ms = (time.perf_counter() - start) * 1000
        if not response:
            return 0, elapsed_ms
        status_line = response.split(b"\r\n", 1)[0].decode(errors="replace")
        parts = status_line.split(" ")
        status_code = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        return status_code, elapsed_ms
    except OSError:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return 0, elapsed_ms


class LoadTestResult:
    def __init__(self):
        self.lock = threading.Lock()
        self.statuses = []
        self.latencies = []

    def record(self, status, latency):
        with self.lock:
            self.statuses.append(status)
            self.latencies.append(latency)

    def report(self, duration):
        total = len(self.statuses)
        if total == 0:
            print("  no requests completed")
            return
        ok = sum(1 for s in self.statuses if s == 200)
        rate_limited = sum(1 for s in self.statuses if s == 429)
        bad_gateway = sum(1 for s in self.statuses if s == 502)
        failed = sum(1 for s in self.statuses if s == 0)
        lat_sorted = sorted(self.latencies)

        def pct(p):
            idx = min(len(lat_sorted) - 1, int(len(lat_sorted) * p))
            return lat_sorted[idx]

        print(f"  total requests : {total}")
        print(f"  throughput     : {total / duration:.1f} req/s")
        print(f"  200 OK         : {ok} ({ok / total * 100:.1f}%)")
        print(f"  429 limited    : {rate_limited} ({rate_limited / total * 100:.1f}%)")
        print(f"  502 bad gateway: {bad_gateway} ({bad_gateway / total * 100:.1f}%)")
        print(f"  conn failed    : {failed} ({failed / total * 100:.1f}%)")
        print(f"  p50 latency    : {pct(0.50):.1f} ms")
        print(f"  p95 latency    : {pct(0.95):.1f} ms")
        print(f"  p99 latency    : {pct(0.99):.1f} ms")


def worker(host, port, n_requests, result, source_ip=None):
    for _ in range(n_requests):
        status, latency = make_request(host, port, source_ip=source_ip)
        result.record(status, latency)


def run_load_test(host, port, concurrency, requests_per_client, label="", distinct_clients=True):
    """distinct_clients=True binds each virtual user to its own 127.x.x.x
    source IP, so the rate limiter sees `concurrency` separate clients
    instead of one shared IP. Set False to deliberately test what happens
    when many users hammer from behind a single IP (e.g. NAT/proxy)."""
    result = LoadTestResult()
    threads = []
    start = time.perf_counter()
    for i in range(concurrency):
        source_ip = f"127.0.{(i // 250) + 1}.{(i % 250) + 1}" if distinct_clients else None
        t = threading.Thread(target=worker, args=(host, port, requests_per_client, result, source_ip))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    duration = time.perf_counter() - start
    print(f"\n=== {label} ({concurrency} clients x {requests_per_client} reqs) ===")
    result.report(duration)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--requests", type=int, default=40)
    args = parser.parse_args()
    run_load_test(args.host, args.port, args.concurrency, args.requests, label="load test")
